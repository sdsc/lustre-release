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
lustre_ko2iblnd_show_net(struct cYAML *lndparams, struct lnet_lnd_tunables *lnd_cfg)
{
	if (cYAML_create_number(lndparams, "map_on_demand",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_map_on_demand) == NULL)
		return -1;

	if (cYAML_create_number(lndparams, "concurrent_sends",
				lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_concurrent_sends) == NULL)
		return -1;

	return 0;
}

void
lustre_ko2iblnd_parse_net(struct cYAML *lndparams, struct lnet_lnd_tunables *lnd_cfg)
{
	struct cYAML *map_on_demand = NULL, *concurrent_sends = NULL;

	map_on_demand = cYAML_get_object_item(lndparams, "map_on_demand");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_map_on_demand =
		(map_on_demand) ? map_on_demand->cy_valueint : -1;

	concurrent_sends = cYAML_get_object_item(lndparams, "concurrent_sends");

	lnd_cfg->lnd_tunables_u.lnd_o2iblnd.lnd_concurrent_sends =
		(concurrent_sends) ? concurrent_sends->cy_valueint : -1;
}
