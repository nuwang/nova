# Copyright (c) 2013 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from nova import db
from nova.scheduler import filters


class SpareZoneFilter(filters.BaseHostFilter):
    """Stops hosts from being created in or moved to a host aggregate that is
    marked as 'is_spare'

    Works with aggregate metadata, using the key 'is_spare'
    """

    def host_passes(self, host_state, filter_properties):
        context = filter_properties['context'].elevated()
        metadata = db.aggregate_metadata_get_by_host(
                     context, host_state.host, key='is_spare')
        is_spare = metadata.get('is_spare', 'false')
        return is_spare.lower() != 'false'
