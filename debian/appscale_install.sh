cd `dirname $0`/..
if [ -z "$APPSCALE_HOME_RUNTIME" ]; then
    export APPSCALE_HOME_RUNTIME=`pwd`
fi

DESTDIR=$2
APPSCALE_HOME=${DESTDIR}${APPSCALE_HOME_RUNTIME}

. debian/appscale_install_functions.sh

echo "Install AppScale into ${APPSCALE_HOME}"
echo "APPSCALE_HOME in runtime=${APPSCALE_HOME_RUNTIME}"

case "$1" in
    core)
	# scratch install of appscale including post script.
	installappscaleprofile
	. /etc/profile.d/appscale.sh
	installgems
	postinstallgems
        installsetuptools
	installhaproxy
	postinstallhaproxy
	installnginx
	postinstallnginx
        installpython27
        installnumpy
        installmatplotlib
        installPIL
        installpycrypto
        installlxml
        installxmpppy
	installjavajdk
        installappserverjava
        postinstallappserverjava
        installmonitoring
	postinstallmonitoring
	installthrift_fromsource
	postinstallthrift_fromsource
        installtornado
        postinstalltornado
	installprotobuf
	postinstallprotobuf
        installflexmock
        installnose
	installhadoop
	postinstallhadoop
	installzookeeper
	postinstallzookeeper
        installrabbitmq
        postinstallrabbitmq
        installcelery
	installservice
	postinstallservice
	setupntpcron
        updatealternatives
	sethosts
        setulimits
<<<<<<< HEAD
=======
        increaseconnections
>>>>>>> e2a9b0a2e1e42ad88baa49052f1e34a0ef380246
	;;
    cassandra)
	installcassandra
	postinstallcassandra
	;;
    hbase)
	installhbase
	postinstallhbase
	;;
    hypertable)
	installhypertable
	postinstallhypertable
	;;
    # for test only. this should be included in core and all.
    zookeeper)
	installzookeeper
	postinstallzookeeper
	;;
    hadoop)
	installhadoop
	postinstallhadoop
	;;
    protobuf-src)
	installprotobuf_fromsource
	postinstallprotobuf
	;;
    rabbit-mq)
        installrabbitmq
        postinstallrabbitmq
        ;; 
    celery)
        installcelery
        ;;
    all)
	# scratch install of appscale including post script.
	installappscaleprofile
	. /etc/profile.d/appscale.sh
	installgems
	postinstallgems
        installsetuptools
	installhaproxy
	postinstallhaproxy
	installnginx
	postinstallnginx
        installpython27
        installnumpy
        installmatplotlib
        installPIL
        installpycrypto
        installlxml
        installxmpppy
	installjavajdk
        installappserverjava
        postinstallappserverjava
        installmonitoring
	postinstallmonitoring
	installthrift_fromsource
	postinstallthrift_fromsource
        installtornado
        installflexmock
        installnose
        postinstalltornado
	installprotobuf
	postinstallprotobuf
	installhadoop
	postinstallhadoop
	installzookeeper
	postinstallzookeeper
        installcassandra
	postinstallcassandra
	installhbase
	postinstallhbase
	installhypertable
	postinstallhypertable
        installrabbitmq
        postinstallrabbitmq
        installcelery
	installservice
	postinstallservice
	updatealternatives
	setupntpcron
        sethosts
        setulimits
<<<<<<< HEAD
=======
        increaseconnections
>>>>>>> e2a9b0a2e1e42ad88baa49052f1e34a0ef380246
	;;
esac
