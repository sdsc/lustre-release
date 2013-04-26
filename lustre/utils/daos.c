#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/ioctl.h>

#if HAVE_LIBPTHREAD
#include <pthread.h>
#endif

#include <liblustre.h>
#include <libcfs/libcfsutil.h>
#include <lnet/lnetctl.h>
#include <lustre/lustre_build_version.h>
#include <daos/daos_lib.h>
#include <obd.h>

#define DAOS_IOC_BLOB		1024

struct daos_ioc_buf {
	char			iob_blob[DAOS_IOC_BLOB];
	char			*iob_buf;
	struct daos_usr_data	iob_ud;
};

struct daos_lib_data {
#if HAVE_LIBPTHREAD
	pthread_mutex_t		dl_lock;
#endif
	int			dl_state;
};

static struct daos_lib_data	daos_ld = {
#if HAVE_LIBPTHREAD
	.dl_lock		= PTHREAD_MUTEX_INITIALIZER,
#endif
	.dl_state		= 0,
};

#if HAVE_LIBPTHREAD
#define daos_entry_lock()	pthread_mutex_lock(&daos_ld.dl_lock)
#define daos_entry_unlock()	pthread_mutex_unlock(&daos_ld.dl_lock)
#else
#define daos_entry_lock()	do {} while (0)
#define daos_entry_unlock()	do {} while (0)
#endif

static int
daos_entry_init_check(void)
{
	int	rc;

	if (likely(daos_ld.dl_state != 0))
		goto out;

	daos_entry_lock();
	if (daos_ld.dl_state != 0) {
		daos_entry_unlock();
		goto out;
	}

	rc = register_ioc_dev(DAOS_DEV_ID, DAOS_DEV_PATH,
			      DAOS_DEV_MAJOR, DAOS_DEV_MINOR);
	daos_ld.dl_state = rc < 0 ? rc : 1;
	daos_entry_unlock();
 out:
	return daos_ld.dl_state < 0 ? daos_ld.dl_state : 0;
}

static inline int
daos_ioc_buf_size(struct daos_ioc_buf *iob)
{
	struct daos_usr_data	*ud = &iob->iob_ud;
	int			len = cfs_size_round(sizeof(*ud));
	int			i;

	for (i = 0; i < DAOS_IOC_INBUF_NUM; i++)
		len += cfs_size_round(ud->ud_inbuf_lens[i]);

	return len;
}

static int
daos_ioc_buf_pack(struct daos_ioc_buf *iob)
{
	struct daos_usr_data	*ud = &iob->iob_ud;
	struct daos_usr_data	*overlay;
	char			*ptr;
	int			rc;
	int			i;

	ud->ud_hdr.ih_len	= daos_ioc_buf_size(iob);
	ud->ud_hdr.ih_version	= DAOS_IOC_VERSION;

	if (ud->ud_hdr.ih_len > DAOS_IOC_BLOB) {
		iob->iob_buf = malloc(ud->ud_hdr.ih_len);
		if (iob->iob_buf == NULL) {
			fprintf(stderr, "Failed to allocate buffer for "
				"daos ioctl\n");
			return -ENOMEM;
		}
	} else {
		iob->iob_buf = iob->iob_blob;
	}

	overlay = (struct daos_usr_data *)iob->iob_buf;
	memcpy(overlay, ud, sizeof(*ud));

	ptr = overlay->ud_body;
	for (i = 0; i < DAOS_IOC_INBUF_NUM; i++) {
		if (ud->ud_inbufs[i] != NULL)
			LOGL(ud->ud_inbufs[i], ud->ud_inbuf_lens[i], ptr);
	}

	rc = daos_ioc_data_parse(overlay);
	if (rc != 0)
		return -EINVAL;

	return 0;
}

static void
daos_ioc_buf_add(struct daos_ioc_buf *iob, unsigned int idx,
		 unsigned int buf_len, void *buf)
{
	LASSERT(idx < DAOS_IOC_INBUF_NUM);
	LASSERT(iob->iob_ud.ud_inbufs[idx] == NULL);

	iob->iob_ud.ud_inbuf_lens[idx] = buf_len;
	iob->iob_ud.ud_inbufs[idx] = buf;
}

static int
daos_ioc_buf_add_pack(struct daos_ioc_buf *iob, unsigned int buf_len, void *buf)
{
	daos_ioc_buf_add(iob, 0, buf_len, buf);
	return daos_ioc_buf_pack(iob);
}

static void
daos_ioc_buf_fini(struct daos_ioc_buf *iob)
{
	if (iob->iob_buf != iob->iob_blob)
		free(iob->iob_buf);
}

static int
daos_ioctl(unsigned int opc, struct daos_ioc_buf *iob)
{
	int	rc;

	rc = daos_entry_init_check();
	if (rc != 0)
		return rc;

	LASSERT(iob->iob_buf != NULL);
	rc = l_ioctl(DAOS_DEV_ID, opc, iob->iob_buf);
	daos_ioc_buf_fini(iob);

	return rc;
}

int
daos_eq_create(daos_handle_t *eqh)
{
	struct daos_usr_eq_create	eqc;
	struct daos_ioc_buf		iob;
	int				rc;

	if (eqh == NULL)
		return -EINVAL;

	memset(&eqc, 0, sizeof(eqc));
	eqc.eqc_handle = eqh;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(eqc), &eqc);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EQ_CREATE, &iob);
	return rc == 0 ? 0 : -errno;
}

int
daos_eq_destroy(daos_handle_t eqh)
{
	struct daos_usr_eq_destroy	eqd;
	struct daos_ioc_buf		iob;
	int				rc;

	if (eqh.cookie == 0)
		return -EINVAL;

	memset(&eqd, 0, sizeof(eqd));
	eqd.eqd_handle = eqh;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(eqd), &eqd);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EQ_DESTROY, &iob);
	return rc == 0 ? 0 : -errno;
}

int
daos_eq_poll(daos_handle_t eqh, int wait_inf,
	     int64_t timeout, int eventn, daos_event_t **events)
{
	struct daos_usr_eq_poll		eqp;
	struct daos_ioc_buf		iob;
	int				rc;

	if (eqh.cookie == 0)
		return -EINVAL;

	if (eventn == 0 || events == NULL)
		return -EINVAL;

	memset(&eqp, 0, sizeof(eqp));
	eqp.eqp_handle	 = eqh;
	eqp.eqp_timeout	 = timeout;
	eqp.eqp_wait_inf = wait_inf;
	eqp.eqp_ueventn	 = eventn;
	eqp.eqp_uevents	 = events;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(eqp), &eqp);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EQ_POLL, &iob);
	return rc >= 0 ? rc : -errno;
}

int
daos_eq_query(daos_handle_t eqh, daos_ev_query_t query,
	      unsigned int eventn, daos_event_t **events)
{
	struct daos_usr_eq_query	eqq;
	struct daos_ioc_buf		iob;
	int				rc;

	if (eqh.cookie == 0)
		return -EINVAL;

	memset(&eqq, 0, sizeof(eqq));
	eqq.eqq_handle	 = eqh;
	eqq.eqq_query	 = query;
	eqq.eqq_ueventn	 = eventn;
	eqq.eqq_uevents	 = events;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(eqq), &eqq);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EQ_QUERY, &iob);
	return rc >= 0 ? rc : -errno;
}

int
daos_event_init(daos_event_t *ev, daos_handle_t eqh, daos_event_t *parent)
{
	struct daos_event_priv	*priv;
	struct daos_usr_ev_init	evi;
	struct daos_ioc_buf	iob;
	int			rc;

	if (eqh.cookie == 0)
		return -EINVAL;

	if (ev == NULL)
		return -EINVAL;

	memset(&evi, 0, sizeof(evi));
	if (parent != NULL) {
		priv = (struct daos_event_priv *)&parent->ev_private.space[0];
		if (priv->ev_parent != 0) /* nested */
			return -EINVAL;

		evi.evi_parent = priv->ev_cookie;
	}

	priv = (struct daos_event_priv *)&ev->ev_private.space[0];
	evi.evi_eqh	= eqh;
	evi.evi_uevent	= ev;
	evi.evi_upriv	= priv;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(evi), &evi);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EV_INIT, &iob);
	if (rc != 0)
		return -errno;

	priv->ev_parent = evi.evi_parent;
	priv->ev_eqh = eqh;
	return 0;
}

int
daos_event_fini(daos_event_t *ev)
{
	struct daos_event_priv	*evp;
	struct daos_usr_ev_fini	evf;
	struct daos_ioc_buf	iob;
	int			rc;

	if (ev == NULL)
		return -EINVAL;

	evp = (struct daos_event_priv *)&ev->ev_private.space[0];
	if (evp->ev_eqh.cookie == 0 || evp->ev_cookie == 0)
		return -EINVAL;

	memset(&evf, 0, sizeof(evf));
	evf.evf_id.cookie = evp->ev_cookie;
	evf.evf_id.eqh = evp->ev_eqh;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(evf), &evf);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EV_FINI, &iob);
	if (rc != 0)
		return -errno;

	memset(ev, 0, sizeof(ev));
	return 0;
}

int
daos_event_abort(daos_event_t *ev)
{
	struct daos_event_priv		*evp;
	struct daos_usr_ev_abort	eva;
	struct daos_ioc_buf		iob;
	int				rc;

	if (ev == NULL)
		return -EINVAL;

	evp = (struct daos_event_priv *)&ev->ev_private.space[0];
	if (evp->ev_eqh.cookie == 0 || evp->ev_cookie == 0)
		return -EINVAL;

	memset(&eva, 0, sizeof(eva));
	eva.eva_id.cookie = evp->ev_cookie;
	eva.eva_id.eqh = evp->ev_eqh;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(eva), &eva);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_EV_ABORT, &iob);
	return rc == 0 ? 0 : -errno;
}

daos_event_t *
daos_event_next(daos_event_t *parent, daos_event_t *child)
{
	daos_event_t			*next;
	struct daos_event_priv		*evp;
	struct daos_usr_ev_next		evn;
	struct daos_ioc_buf		iob;
	int				rc;

	if (parent == NULL)
		return NULL;

	evp = (struct daos_event_priv *)&parent->ev_private.space[0];
	if (evp->ev_eqh.cookie == 0 || evp->ev_cookie == 0)
		return NULL;

	memset(&evn, 0, sizeof(evn));
	evn.evn_parent.eqh	= evp->ev_eqh;
	evn.evn_parent.cookie	= evp->ev_cookie;
	evn.evn_next		= &next;

	if (child != NULL) { /* not the first call */
		evp = (struct daos_event_priv *)&child->ev_private.space[0];
		if (evp->ev_eqh.cookie != evn.evn_parent.eqh.cookie ||
		    evp->ev_cookie == 0)
			return NULL;

		evn.evn_current = evp->ev_cookie;
	}

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(evn), &evn);
	if (rc != 0)
		return NULL;

	rc = daos_ioctl(DAOS_IOC_EV_ABORT, &iob);
	if (rc != 0)
		return NULL;

	return next;
}

int
daos_noop(daos_event_t *ev, int latency)
{
	struct daos_event_priv	*evp;
	struct daos_usr_noop	unp;
	struct daos_ioc_buf	iob;
	int			rc;

	evp = (struct daos_event_priv *)&ev->ev_private.space[0];
	if (evp->ev_eqh.cookie == 0 || evp->ev_cookie == 0)
		return -EINVAL;

	memset(&unp, 0, sizeof(unp));
	unp.unp_ev.cookie	= evp->ev_cookie;
	unp.unp_ev.eqh		= evp->ev_eqh;
	unp.unp_latency		= latency;

	memset(&iob, 0, sizeof(iob));
	rc = daos_ioc_buf_add_pack(&iob, sizeof(unp), &unp);
	if (rc != 0)
		return rc;

	rc = daos_ioctl(DAOS_IOC_NOOP, &iob);
	return rc == 0 ? 0 : -errno;
}
