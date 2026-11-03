/* Exercise the actual pinned cache workers and nbdkit FLUSH/FUA callbacks.
 * Link wrapping observes a wait while the cache mutex is held; backend gates
 * establish WRITING deterministically. No sleeps are used as synchronization. */
#include "nbdkit.c"

#define REQUIRE(c) do { if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)
extern void test_wait_clean(struct s3backer_store *);
#define N 8
static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t changed = PTHREAD_COND_INITIALIZER;
static int entered[N], permits[N], completed[N], fail_first, cancel_first, release_all;
static unsigned char persisted[N][4096];
static _Thread_local int in_flush, wire_error;
static int waiting, done, flush_result, flush_errno, inner_error, inner_calls;
static int use_fua, explicit_flush;
static struct s3backer_store *cache;

extern int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
extern int __real_pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *, const struct timespec *);
static void observe_wait(void) {
    if (!in_flush) return;
    pthread_mutex_lock(&gate); waiting++; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
}
int __wrap_pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) {
    observe_wait(); return __real_pthread_cond_wait(c, m);
}
int __wrap_pthread_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m, const struct timespec *t) {
    observe_wait(); return __real_pthread_cond_timedwait(c, m, t);
}
void nbdkit_error(const char *fmt, ...) { (void)fmt; }
void nbdkit_set_error(int error) { wire_error = error; }
static void logger(int level, const char *fmt, ...) { (void)level; (void)fmt; }
static int simple(struct s3backer_store *s) { (void)s; return 0; }
static void destroy(struct s3backer_store *s) { (void)s; }
static int backend_read(struct s3backer_store *s, s3b_block_t b, void *dst, u_char *tag, const u_char *expected, int strict) {
    (void)s; (void)expected; (void)strict;
    pthread_mutex_lock(&gate); memcpy(dst, persisted[b],4096); pthread_mutex_unlock(&gate);
    if(tag) memset(tag,0,MD5_DIGEST_LENGTH); return 0;
}
static int backend_write(struct s3backer_store *s, s3b_block_t b, const void *src, u_char *tag, check_cancel_t *cancel, void *arg) {
    (void)s; (void)cancel; (void)arg;
    pthread_mutex_lock(&gate); int call=++entered[b]; pthread_cond_broadcast(&changed);
    while (!release_all && permits[b] < call) pthread_cond_wait(&changed,&gate);
    int r=(fail_first && b==0 && call==1) ? EIO : 0;
    if(cancel_first && b==0 && call==1) r=ECONNABORTED;
    if (!r) { if(src) memcpy(persisted[b],src,4096); else memset(persisted[b],0,4096); }
    completed[b]=call; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
    if(tag) memset(tag,0,MD5_DIGEST_LENGTH); return r;
}
static int backend_flush(struct s3backer_store *s, const s3b_block_t *b, u_int n, long t) {
    (void)s; (void)b; (void)n; (void)t; inner_calls++; return inner_error;
}
static void watchdog(int sig) { (void)sig; static const char msg[]="FAIL watchdog\n"; ssize_t ignored=write(2,msg,sizeof(msg)-1); (void)ignored; _exit(124); }
static void wait_enter(int b,int count) {
    pthread_mutex_lock(&gate); while(entered[b]<count) pthread_cond_wait(&changed,&gate); pthread_mutex_unlock(&gate);
}
static void permit(int b,int count) {
    pthread_mutex_lock(&gate); permits[b]=count; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
}
static void *do_flush(void *unused) {
    (void)unused; in_flush=1; wire_error=0;
    int r;
    if (use_fua) { unsigned char buf[4096]; memset(buf,0x72,sizeof(buf)); r=s3b_nbd_plugin_pwrite(NULL,buf,sizeof(buf),0,NBDKIT_FLAG_FUA); }
    else if(explicit_flush) { s3b_block_t b=0; r=cache->flush_blocks(cache,&b,1,1000); }
    else r=s3b_nbd_plugin_flush(NULL,0);
    pthread_mutex_lock(&gate); flush_result=r; flush_errno=wire_error; done=1; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
    return NULL;
}
static pthread_t begin_flush(void) {
    pthread_t t; waiting=done=0; REQUIRE(pthread_create(&t,NULL,do_flush,NULL)==0); return t;
}
static void require_waiting(void) {
    pthread_mutex_lock(&gate); while(!waiting && !done) pthread_cond_wait(&changed,&gate);
    int premature=done; pthread_mutex_unlock(&gate); REQUIRE(!premature);
}
static void write_value(int b,int value) {
    unsigned char buf[4096]; memset(buf,value,sizeof(buf)); REQUIRE(cache->write_block(cache,b,buf,NULL,NULL,NULL)==0);
}
static void verify(int b,int value) {
    for(int i=0;i<4096;i++) REQUIRE(persisted[b][i]==value);
}
int main(int argc,char **argv) {
    REQUIRE(argc==2); signal(SIGALRM,watchdog); alarm(8);
    struct s3backer_store inner={.create_threads=simple,.read_block=backend_read,.write_block=backend_write,.flush_blocks=backend_flush,.shutdown=simple,.destroy=destroy};
    struct block_cache_conf cc={.block_size=4096,.cache_size=8,.num_threads=2,.write_delay=0,.log=logger};
    cc.cache_file=getenv("S3BACKER_TEST_CACHE");
    cc.perform_flush=1; cc.recover_dirty_blocks=1;
    struct s3b_config cfg={.block_size=4096,.file_size=N*4096};
    struct fuse_ops_private fp={0}; config=&cfg; fuse_priv=&fp;
    cache=block_cache_create(&cc,&inner); REQUIRE(cache); fp.s3b=cache; REQUIRE(cache->create_threads(cache)==0);
    pthread_t thread;
    if (!strcmp(argv[1],"empty-lower-error")) {
        inner_error=ENOSPC; REQUIRE(s3b_nbd_plugin_flush(NULL,0)==-1); REQUIRE(wire_error==ENOSPC); REQUIRE(inner_calls==1); inner_error=0;
    } else if (!strcmp(argv[1],"writing") || !strcmp(argv[1],"fua") || !strcmp(argv[1],"timeout")) {
        if(!strcmp(argv[1],"fua")) {use_fua=1; thread=begin_flush(); wait_enter(0,1);}
        else { write_value(0,0x61); wait_enter(0,1); explicit_flush=!strcmp(argv[1],"timeout"); thread=begin_flush(); }
        require_waiting();
        if(explicit_flush) { REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==ETIMEDOUT); permit(0,1); REQUIRE(s3b_nbd_plugin_flush(NULL,0)==0); }
        else {permit(0,1); REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==0);}
        verify(0,use_fua ? 0x72 : 0x61);
    } else if(!strcmp(argv[1],"concurrent-writing")) {
        write_value(0,0x31); write_value(1,0x41); wait_enter(0,1); wait_enter(1,1);
        thread=begin_flush(); require_waiting(); permit(0,1); permit(1,1);
        REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==0); verify(0,0x31); verify(1,0x41);
    } else if(!strcmp(argv[1],"error-quick-retry")) {
        fail_first=1; write_value(0,0x53); wait_enter(0,1); thread=begin_flush();
        pthread_mutex_lock(&gate); while(!waiting && !done) pthread_cond_wait(&changed,&gate); pthread_mutex_unlock(&gate);
        permit(0,2); REQUIRE(pthread_join(thread,NULL)==0);
        REQUIRE(flush_result==-1); REQUIRE(flush_errno==EIO); test_wait_clean(cache); verify(0,0x53);
    } else if(!strcmp(argv[1],"cancel-rewrite")) {
        cancel_first=1; write_value(0,0x31); wait_enter(0,1); write_value(0,0x32);
        thread=begin_flush(); require_waiting(); permit(0,1); wait_enter(0,2); permit(0,2);
        REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==0); verify(0,0x32);
    } else if(!strcmp(argv[1],"error-retry")) {
        fail_first=1; write_value(0,0x53); wait_enter(0,1); thread=begin_flush();
        /* Baseline returns success here with the backend still blocked. */
        pthread_mutex_lock(&gate); while(!waiting && !done) pthread_cond_wait(&changed,&gate); pthread_mutex_unlock(&gate);
        permit(0,1); REQUIRE(pthread_join(thread,NULL)==0);
        REQUIRE(flush_result==-1); REQUIRE(flush_errno==EIO);
        wait_enter(0,2);
        struct block_cache_stats st; block_cache_get_stats(cache,&st); REQUIRE(st.dirty_ratio>0);
        unsigned char got[4096]; REQUIRE(cache->read_block(cache,0,got,NULL,NULL,0)==0);
        for(int i=0;i<4096;i++) REQUIRE(got[i]==0x53);
        permit(0,2);
        pthread_mutex_lock(&gate); while(completed[0]<2) pthread_cond_wait(&changed,&gate); pthread_mutex_unlock(&gate);
        /* Wait for the real worker's success transition (not just backend return). */
        test_wait_clean(cache);
        REQUIRE(s3b_nbd_plugin_flush(NULL,0)==0); verify(0,0x53);
    } else if(!strcmp(argv[1],"multi-writing2")) {
        write_value(0,0x31); write_value(1,0x41); wait_enter(0,1); wait_enter(1,1);
        write_value(0,0x32); write_value(2,0x51); /* WRITING2 + WRITING + DIRTY */
        thread=begin_flush(); require_waiting(); permit(1,1); wait_enter(2,1); permit(2,1); permit(0,1);
        wait_enter(0,2); /* a flush must include the replacement generation */
        pthread_mutex_lock(&gate); REQUIRE(!done); pthread_mutex_unlock(&gate);
        permit(0,2); REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==0);
        verify(0,0x32); verify(1,0x41); verify(2,0x51);
    } else if(!strcmp(argv[1],"lower-error")) {
        write_value(0,0x61); wait_enter(0,1); inner_error=ENOSPC; thread=begin_flush();
        require_waiting(); permit(0,1); REQUIRE(pthread_join(thread,NULL)==0); REQUIRE(flush_result==-1); REQUIRE(flush_errno==ENOSPC); inner_error=0;
    } else { REQUIRE(0); }
    pthread_mutex_lock(&gate); release_all=1; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
    REQUIRE(cache->shutdown(cache)==0); cache->destroy(cache); alarm(0); printf("PASS %s (actual bridge/cache, normal close)\n",argv[1]); return 0;
}
