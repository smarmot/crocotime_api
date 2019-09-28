import requests
import json


class Crocotime:
    def __init__(self, url, headers, token, app_version):
        self.url = url
        self.headers = headers
        self.token = token
        self.app_version = app_version

    def request(self, controller, query=None):
        if query:
            request = dict(server_token=self.token, app_version=self.app_version, controller=controller, query=query)
        else:
            request = dict(server_token=self.token, app_version=self.app_version, controller=controller)

        response = requests.post(self.url, data=json.dumps(request), headers=self.headers).json()
        return response

    def get_employees(self):
        return self.request('api_employees')['result']['items']

    def get_departments(self):
        return self.request('api_departments')['result']['items']

    def get_window_switch_train(self, interval_begin, interval_end, employee_id):
        response = self.request('api_window_switch_train',
                                {"interval": [interval_begin, interval_end], "employee_id": employee_id})['result']['activities']
        # к каждой строке ответа добавим employee_id
        return [dict(item,
                     employee_id=employee_id,
                     interval_begin=item['interval'][0],
                     interval_end=item['interval'][1])
                for item in response]

    # TODO обработка исключений, если в функцию был передан аргумент не того типа
    def get_programs(self, program_ids):
        query = {"table": "programs", "columns": ["program_id", "name", "program_group_id"], "ids": program_ids}
        return self.request('api_table_controller', query)['result']['items']
        
    def _program_groups_by_id(self, program_group_ids):
        """
        :return: program_groups by id
        """
        query = {"table": "program_groups", "columns": ["program_group_id", "display_name", "parent_group_id"], "ids": program_group_ids}
        return self.request('api_table_controller', query)['result']['items']

    def get_program_groups(self, program_group_ids):
        """

        :return: program_groups, to root (parent_group_id = -1)
        """

        program_groups = []

        def _parent_groups(_program_group_ids):
            _program_groups = self._program_groups_by_id(_program_group_ids)
            _parents_ids = list(set(p['parent_group_id'] for p in _program_groups
                                    if not p['parent_group_id'] == -1
                                    and p['parent_group_id'] not in _program_group_ids))
                                                         
            program_groups.extend(_program_groups)
            if not len(_parents_ids) == 0:
                _parent_groups(_parents_ids)
        
        _parent_groups(program_group_ids)
        
        return program_groups

    def get_windows(self, window_ids):
        query = {"table": "windows", "columns": ["window_id", "title"], "ids": window_ids}
        return self.request('api_table_controller', query)['result']['items']

    def get_employee_activity(self, interval_begin, interval_end, employee_ids):
        query = {"interval": [interval_begin, interval_end], "employees": employee_ids}
        response = self.request('api_employee_activity', query)['result']['items']

        # к каждой строке ответа добавим временной интервал
        return [dict(item,
                     interval_begin=interval_begin,
                     interval_end=interval_end)
                for item in response]

    def get_employees_work_periods(self, day_begin, employee_ids):
        query = {"day": day_begin, "employees": employee_ids}
        return self.request('api_employee_work_periods', query)['result']['items']
