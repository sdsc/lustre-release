#
# LUSTRE_VERSION_CPP_MACROS
#
AC_DEFUN([LUSTRE_VERSION_CPP_MACROS], [
LUSTRE_MAJOR=`echo AC_PACKAGE_VERSION | sed -re ['s/([0-9]+)\.([0-9]+)\.([0-9]+)(\.([0-9]+))?.*/\1/']`
LUSTRE_MINOR=`echo AC_PACKAGE_VERSION | sed -re ['s/([0-9]+)\.([0-9]+)\.([0-9]+)(\.([0-9]+))?.*/\2/']`
LUSTRE_PATCH=`echo AC_PACKAGE_VERSION | sed -re ['s/([0-9]+)\.([0-9]+)\.([0-9]+)(\.([0-9]+))?.*/\3/']`
LUSTRE_FIX=`echo AC_PACKAGE_VERSION | sed -re ['s/([0-9]+)\.([0-9]+)\.([0-9]+)(\.([0-9]+))?.*/\5/']`
AS_IF([test -z "$LUSTRE_FIX"], [LUSTRE_FIX="0"])

AC_DEFINE_UNQUOTED([LUSTRE_MAJOR], "$LUSTRE_MAJOR", [First number in the Lustre version])
AC_DEFINE_UNQUOTED([LUSTRE_MINOR], "$LUSTRE_MINOR", [Second number in the Lustre version])
AC_DEFINE_UNQUOTED([LUSTRE_PATCH], "$LUSTRE_PATCH", [Third number in the Lustre version])
AC_DEFINE_UNQUOTED([LUSTRE_FIX], "$LUSTRE_FIX", [Fourth number in the Lustre version])
])
