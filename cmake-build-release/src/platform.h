#define THREAD_LOCAL __thread
#define ATTRIBUTE_NORETURN __attribute__ ((noreturn))
#define ATTRIBUTE_NOINLINE __attribute__ ((noinline))

#define GCC_VERSION (__GNUC__ * 10000 \
                     + __GNUC_MINOR__ * 100 \
                     + __GNUC_PATCHLEVEL__)

/* #undef LIBUNWIND_FOUND */
/* #undef HAVE_DLADDR */
#define OS_LINUX
/* #undef OS_MACOSX */
#define ENABLE_FRAME_POINTER
/* #undef HAVE_SYMBOLIZE */
/* #undef NEED_BOOST */
/* #undef STRERROR_R_RETURN_INT */
#define HAVE_STEADY_CLOCK
#define HAVE_NESTED_EXCEPTION
/* #undef HAVE_BOOST_CHRONO */
#define HAVE_STD_CHRONO
/* #undef HAVE_BOOST_ATOMIC */
#define HAVE_STD_ATOMIC

// defined by gcc
#if defined(__ELF__) && defined(OS_LINUX)
# define HAVE_SYMBOLIZE
#elif defined(OS_MACOSX) && defined(HAVE_DLADDR)
// Use dladdr to symbolize.
# define HAVE_SYMBOLIZE
#endif

#define STACK_LENGTH 64
