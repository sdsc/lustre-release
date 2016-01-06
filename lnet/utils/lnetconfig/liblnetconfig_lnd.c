/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 *
 * LGPL HEADER END
 *
 * Copyright (c) 2015, James Simmons
 *
 * Author:
 *   James Simmons <jsimmons@infradead.org>
 */
#include <stdio.h>
#include <libcfs/util/ioctl.h>
#include "liblnetconfig.h"
#include "cyaml.h"

int
lustre_ko2iblnd_show_net(struct cYAML *lndparams,
			 struct lnet_lnd_tunables *lnd_cfg)
{
	if (cYAML_create_number(lndparams, "peercredits_hiw",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_peercredits_hiw) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "map_on_demand",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_map_on_demand) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "concurrent_sends",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_concurrent_sends) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "fmr_pool_size",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_pool_size) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "fmr_flush_trigger",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_flush_trigger) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "fmr_cache",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_cache) == NULL)
		return -1;
	return 0;
}

void
lustre_ko2iblnd_parse_net(struct cYAML *lndparams,
			  struct lnet_lnd_tunables *lnd_cfg)
{
	struct cYAML *map_on_demand = NULL, *concurrent_sends = NULL;
	struct cYAML *fmr_pool_size = NULL, *fmr_cache = NULL;
	struct cYAML *fmr_flush_trigger = NULL;

	map_on_demand = cYAML_get_object_item(lndparams, "map_on_demand");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_map_on_demand =
		(map_on_demand) ? map_on_demand->cy_valueint : 0;

	concurrent_sends = cYAML_get_object_item(lndparams, "concurrent_sends");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_concurrent_sends =
		(concurrent_sends) ? concurrent_sends->cy_valueint : 0;

	fmr_pool_size = cYAML_get_object_item(lndparams, "fmr_pool_size");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_pool_size =
		(fmr_pool_size) ? fmr_pool_size->cy_valueint : 0;

	fmr_flush_trigger = cYAML_get_object_item(lndparams, "fmr_flush_trigger");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_flush_trigger =
		(fmr_flush_trigger) ? fmr_flush_trigger->cy_valueint : 0;

	fmr_cache = cYAML_get_object_item(lndparams, "fmr_cache");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_fmr_cache =
		(fmr_cache) ? fmr_cache->cy_valueint : 0;
}
