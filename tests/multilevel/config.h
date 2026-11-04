/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* FUSE API version */
#define FUSE_USE_VERSION 35

/* Define to 1 if you have the <assert.h> header file. */
#define HAVE_ASSERT_H 1

/* Define to 1 if you have the <ctype.h> header file. */
#define HAVE_CTYPE_H 1

/* Define to 1 if you have the <curl/curl.h> header file. */
#define HAVE_CURL_CURL_H 1

/* Define to 1 if you have the declaration of `fallocate', and to 0 if you
   don't. */
#define HAVE_DECL_FALLOCATE 1

/* Define to 1 if you have the declaration of `FALLOC_FL_KEEP_SIZE', and to 0
   if you don't. */
#define HAVE_DECL_FALLOC_FL_KEEP_SIZE 1

/* Define to 1 if you have the declaration of `FALLOC_FL_PUNCH_HOLE', and to 0
   if you don't. */
#define HAVE_DECL_FALLOC_FL_PUNCH_HOLE 1

/* Define to 1 if you have the declaration of `fdatasync', and to 0 if you
   don't. */
#define HAVE_DECL_FDATASYNC 1

/* Define to 1 if you have the declaration of `posix_fadvise', and to 0 if you
   don't. */
#define HAVE_DECL_POSIX_FADVISE 1

/* Define to 1 if you have the declaration of `prctl', and to 0 if you don't.
   */
#define HAVE_DECL_PRCTL 1

/* Define to 1 if you have the declaration of `PR_SET_IO_FLUSHER', and to 0 if
   you don't. */
#define HAVE_DECL_PR_SET_IO_FLUSHER 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the <errno.h> header file. */
#define HAVE_ERRNO_H 1

/* Define to 1 if you have the <err.h> header file. */
#define HAVE_ERR_H 1

/* Define to 1 if you have the <expat.h> header file. */
#define HAVE_EXPAT_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#define HAVE_LIBCRYPTO 1

/* Define to 1 if you have the `curl' library (-lcurl). */
#define HAVE_LIBCURL 1

/* Define to 1 if you have the `expat' library (-lexpat). */
#define HAVE_LIBEXPAT 1

/* Define to 1 if you have the `fuse3' library (-lfuse3). */
#define HAVE_LIBFUSE3 1

/* Define to 1 if you have the `pthread' library (-lpthread). */
/* #undef HAVE_LIBPTHREAD */

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define to 1 if you have the <openssl/bio.h> header file. */
#define HAVE_OPENSSL_BIO_H 1

/* Define to 1 if you have the <openssl/buffer.h> header file. */
#define HAVE_OPENSSL_BUFFER_H 1

/* Define to 1 if you have the <openssl/evp.h> header file. */
#define HAVE_OPENSSL_EVP_H 1

/* Define to 1 if you have the <openssl/hmac.h> header file. */
#define HAVE_OPENSSL_HMAC_H 1

/* Define to 1 if you have the <openssl/md5.h> header file. */
#define HAVE_OPENSSL_MD5_H 1

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define to 1 if you have the <stdarg.h> header file. */
#define HAVE_STDARG_H 1

/* Define to 1 if you have the <stddef.h> header file. */
#define HAVE_STDDEF_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdio.h> header file. */
#define HAVE_STDIO_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <syslog.h> header file. */
#define HAVE_SYSLOG_H 1

/* Define to 1 if you have the <sys/queue.h> header file. */
#define HAVE_SYS_QUEUE_H 1

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#define HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the <zlib.h> header file. */
#define HAVE_ZLIB_H 1

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* path to modprobe(8) executable */
#define MODPROBE_EXECUTABLE ""

/* Whether NBDKit is available */
#define NBDKIT 1

/* path to nbdkit(1) executable */
#define NBDKIT_EXECUTABLE "/var/tmp/s3-alt-run-s3backer-4c1a/tools/usr/bin/nbdkit"

/* path to nbd-client(8) executable */
#define NBD_CLIENT_EXECUTABLE "/var/tmp/s3-alt-run-s3backer-4c1a/tools/sbin/nbd-client"

/* disable assertions */
#define NDEBUG 1

/* Name of package */
#define PACKAGE "s3backer"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "https://github.com/archiecobbs/s3backer"

/* Define to the full name of this package. */
#define PACKAGE_NAME "s3backer FUSE filesystem backed by Amazon S3"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "s3backer FUSE filesystem backed by Amazon S3 2.1.6"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "s3backer"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "2.1.6"

/* nbdkit UNIX socket file directory */
#define S3B_NBD_DIR "/run/s3backer-nbd"

/* Define to 1 if all of the C90 standard headers exist (not just the ones
   required in a freestanding environment). This macro is provided for
   backward compatibility; new code need not use it. */
#define STDC_HEADERS 1

/* Version number of package */
#define VERSION "2.1.6"

/* Whether zstd is available */
/* #undef ZSTD */

/* BSD functions */
#define _BSD_SOURCE 1

/* MacOS functions */
#define _DARWIN_C_SOURCE 1

/* Default functions */
#define _DEFAULT_SOURCE 1

/* GNU functions */
#define _GNU_SOURCE 1
