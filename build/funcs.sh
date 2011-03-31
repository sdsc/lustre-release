# autodetect used Distro
autodetect_distro() {

    local name
    local version

    if which lsb_release >/dev/null 2>&1; then
        name="$(lsb_release -s -i)"
        version="$(lsb_release -s -r)"
        case "$name" in
            "EnterpriseEnterpriseServer")
                name="oel"
                version="${version%%.*}"
                ;;
            "RedHatEnterpriseServer" | "ScientificSL" | "CentOS")
                name="rhel"
                version="${version%%.*}"
                ;;
            "SUSE LINUX")
                name="sles"
                ;;
            *)
                fatal 1 "I don't know what distro name $name and version $version is.\nEither update autodetect_distro() or use the --distro argument."
                ;;
        esac
    else
        echo "You really ought to install lsb_release for accurate distro identification"
        # try some heuristics
        if [ -f /etc/SuSE-release ]; then
            name=sles
            version=$(grep ^VERSION /etc/SuSE-release)
            version=${version#*= }
        elif [ -f /etc/redhat-release ]; then
            #name=$(head -1 /etc/redhat-release)
            name=rhel
            version=$(echo "$distroname" |
                      sed -e 's/^[^0-9.]*//g' | sed -e 's/[ \.].*//')
        fi
        if [ -z "$name" -o -z "$version" ]; then
            fatal 1 "I don't know how to determine distro type/version.\nEither update autodetect_distro() or use the --distro argument."
        fi
    fi

    echo ${name}${version}
    return 0

}

# autodetect target
autodetect_target() {
    local distro="$1"

    local target=""
    case ${distro} in
          oel5) target="2.6-oel5";;
         rhel5) target="2.6-rhel5";;
        sles10) target="2.6-sles10";;
        sles11) target="2.6-sles11";;
            *) fatal 1 "I don't know what distro $distro is.\nEither update autodetect_target() or use the --target argument.";;
    esac

    echo ${target}
    return 0

}
