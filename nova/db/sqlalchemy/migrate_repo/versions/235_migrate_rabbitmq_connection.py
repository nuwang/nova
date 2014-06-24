# Copyright (c) 2014 The University of Melbourne
# All Rights Reserved
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

from sqlalchemy import MetaData, Column, Table
from sqlalchemy import Text
from sqlalchemy.sql import select, update
import urlparse


def upgrade(migrate_engine):
    """"""
    meta = MetaData(bind=migrate_engine)
    table = Table('cells', meta, autoload=True)
    result = select(columns=[table.c.id, table.c.transport_url]).execute()
    for id, transport_url in result:
        urls = [urlparse.urlparse(url) for url in transport_url.split (',')]
        url_netloc = ','.join([url.netloc for url in urls])
        url = urlparse.urlunsplit((urls[0].scheme, url_netloc, urls[0].path,
                                   urls[0].query, urls[0].fragment))
        table.update() \
            .values(transport_url=url) \
            .where(table.c.id == id).execute()



def downgrade(migrate_engine):
    """"""
    meta = MetaData(bind=migrate_engine)
    table = Table('cells', meta, autoload=True)
    result = select(columns=[table.c.id, table.c.transport_url]).execute()
    for id, transport_url in result:
        url = urlparse.urlparse(transport_url)
        urls = []
        for netloc in url.netloc.split(','):
            urls.append(urlparse.urlunsplit((url.scheme, netloc, url.path,
                                             url.query, url.fragment)))
        table.update() \
            .values(transport_url=','.join(urls)) \
            .where(table.c.id == id).execute()
