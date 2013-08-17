# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import copy
import json
import webob.exc

import glance.db
import glance.gateway
import glance.notifier
import glance.schema
import glance.store

from glance.api import policy
from glance.common import wsgi
from glance.common import exception
from glance.common import utils
from glance.openstack.common import timeutils


class TasksController(object):
    """Manages operations on tasks."""

    def __init__(self, db_api=None, policy_enforcer=None, notifier=None,
                 store_api=None):
        self.db_api = db_api or glance.db.get_api()
        self.db_api.setup_db_env()
        self.policy = policy_enforcer or policy.Enforcer()
        self.notifier = notifier or glance.notifier.Notifier()
        self.store_api = store_api or glance.store
        self.gateway = glance.gateway.Gateway(self.db_api, self.store_api,
                                              self.notifier, self.policy)

    @utils.mutating
    def create(self, req, task):
        task_factory = self.gateway.get_task_factory(req.context)
        task_repo = self.gateway.get_task_repo(req.context)
        try:
            task = task_factory.new_task(req, task)
            task_repo.add(task)
            task.run()
        except exception.Forbidden as e:
            raise webob.exc.HTTPForbidden(explanation=unicode(e))

        return task

    def index(self, req):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.list()

    def get(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.get(task_id)

    def kill(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        task = task_repo.get(task_id)
        task.remove()
        task_repo.save(task)
        return task


class RequestDeserializer(wsgi.JSONRequestDeserializer):
    _disallowed_properties = ['direct_url', 'self', 'file', 'schema']
    _base_properties = ['type', 'status', 'owner']

    def _get_request_body(self, request):
        output = super(RequestDeserializer, self).default(request)
        if 'body' not in output:
            msg = _('Body expected in request.')
            raise webob.exc.HTTPBadRequest(explanation=msg)
        return output['body']

    @classmethod
    def _check_allowed(cls, task):
        for key in cls._disallowed_properties:
            if key in task:
                msg = "Attribute \'%s\' is read-only." % key
                raise webob.exc.HTTPForbidden(explanation=unicode(msg))

    def __init__(self, schema=None):
        super(RequestDeserializer, self).__init__()
        self.schema = schema or get_schema()

    def index(self):
        pass

    def create(self, request):
        body = self._get_request_body(request)
        self._check_allowed(body)
        task = {}
        properties = body
        for key in self._base_properties:
            try:
                task[key] = properties.pop(key)
            except KeyError:
                pass
        return dict(task=task)


class ResponseSerializer(wsgi.JSONResponseSerializer):
    def __init__(self, schema=None):
        super(ResponseSerializer, self).__init__()
        self.schema = schema or get_schema()

    def _format_task(self, task):
        task_view = {}
        attributes = ['type', 'status', 'owner', 'message']
        for key in attributes:
            task_view[key] = getattr(task, key)
        task_view['id'] = task.task_id
        task_view['created_at'] = timeutils.isotime(task.created_at)
        task_view['updated_at'] = timeutils.isotime(task.updated_at)
        task_view['schema'] = '/v2/schemas/task'
        task_view = self.schema.filter(task_view)  # domain
        return task_view

    def index(self):
        pass

    def create(self, response, task):
        response.status_int = 201
        self.get(response, task)

    def get(self, response, task):
        task_view = self._format_task(task)
        body = json.dumps(task_view, ensure_ascii=False)
        response.unicode_body = unicode(body)
        response.content_type = 'application/json'


_TASK_SCHEMA = {
    "id": {
        "description": "An identifier for the task",
        "pattern": ('^([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}'
                    '-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}$'),
        "type": "string"
     },
    "owner": {
        "description": "An identifier for the owner of this task",
        "type": "string"
    },
    "type": {
        "description": "The type of task represented by this content",
        "enum": [
            "import",
            "export",
            "clone"
        ],
        "type": "string"
    },
    "status": {
        "description": "The current status of this task.",
        "enum": [
            "queued",
            "processing",
            "success",
            "failure"
        ],
        "type": "string"
    },
    "expires_at": {
        "description": "Datetime when this resource is subject to removal",
        "type": "string",
        "required": False
    },
    "message": {
       "description": "Human-readable informative message only included ' \
       ' when appropriate (usually on failure)",
       "type": "string",
       "required": False,
    }
}


def get_schema():
    properties = copy.deepcopy(_TASK_SCHEMA)
    schema = glance.schema.Schema('task', properties)
    return schema


def get_collection_schema():
    member_schema = get_schema()
    return glance.schema.CollectionSchema('tasks', member_schema)


def create_resource():
    """Task resource factory method"""
    schema = get_schema()
    deserializer = RequestDeserializer(schema)
    serializer = ResponseSerializer(schema)
    controller = TasksController()
    return wsgi.Resource(controller, deserializer, serializer)
