# This file contain all configuration information to build
# `lustre-release/lustre/contrib/wireshark'

###########################################################################
#                                                                         #
#    DOWNLOAD CONFIGURATION
#                                                                         #
###########################################################################

##   BEGIN: -can-edit   ##
    # URL of directory containing all source tar balls
export WS_DOWNLOAD_BASE_URL='http://wiresharkdownloads.riverbed.com/wireshark/src/all-versions'

    # wireshark verion to be used
export WS_VERSION='1.6.8'
##   END  : -can-edit   ##

    # URL of the wireshark source code tarball
    # Implicit assumption: Wireshark release names follow the nameing
    # convention coded in the content of the following varialble
export WS_SOURCE_URL="${WS_DOWNLOAD_BASE_URL}/wireshark-${WS_VERSION}.tar.bz2"


###########################################################################
#                                                                         #
#                   BUILD ENVIRONMENT                                     #
#                                                                         #
###########################################################################

##   BEGIN: -can-edit   ##
    # Space separate list of RPMs needed to be installed for 
    # compilation of wireshark
export PREREQUISITE_RPMS='gtk2 gtk2-devel glib2 libpcap libpcap-devel perl'

    # Include and linker flags needed to Lustre/LNet
    # Only version indepent information should be added here
    # (Back ticked expression will be evaluated by make command)
export PLUGIN_COMPILE_FLAGS='`pkg-config --libs --cflags glib-2.0`'
##   END  : -can-edit   ##

    # Top-level directory to be used to unpack/compile/install wireshark/lustre-git-repo
export BUILD_DIR=`pwd`

    # Directory location of wireshark source code
export WS_HOME="${BUILD_DIR}/wireshark-${WS_VERSION}"

    # Lustre GIT uri
export LUSTRE_GIT='git://git.whamcloud.com/fs/lustre-release.git'

    # (Relative) path of the wireshark contribution directory
export LUSTRE_WS_DIR='lustre-release/lustre/contrib/wireshark'

    # Temporary (chroot-ed) installation directory for wireshark RPM install
export WS_PREFIX="${BUILD_DIR}/wireshark-install"

    # RPM internal name for the Lustre/LNet plugins
export PLUGIN_RPM_NAME='lustre-wireshark-plugins'

    # TAR command + options to be used to create a bzip2 tarball
export TAR='/bin/tar jcpf ' 
    # TAR command + options to be used to unpack a bzip2 tarball 
export UNTAR='/bin/tar jxpf ' 
