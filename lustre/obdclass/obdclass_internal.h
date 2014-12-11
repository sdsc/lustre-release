#ifndef __OBDCLASS_INTERNAL__
#define __OBDCLASS_INTERNAL__

extern rwlock_t obd_dev_lock;

struct obd_export *class_new_export_self(struct obd_device *obddev,
					 struct obd_uuid *cluuid);

#endif
