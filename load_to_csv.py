import time
from calendar import timegm
import sys
import os
from multiprocessing import Pool

import crocotime_api
import config
from utils.writer import write_file, append_to_file
from utils.croco import flat_departments


def load(datetime_begin=config.DATETIME_BEGIN, datetime_end=config.DATETIME_END,
         datetime_format=config.DATETIME_FORMAT):
    time_begin = timegm(time.strptime(datetime_begin, datetime_format))
    time_end = timegm(time.strptime(datetime_end, datetime_format))

    if not os.path.exists(config.PATH):
        print('Путь к папке не указан или указан не правильно.')
        print('Использую настройки по умолчанию:')

        folder_name = 'export_files'
        path = os.path.join(os.getcwd(), folder_name)
        print(' ' * 4, path, '\n')

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            print('Создана папка export_files', '\n')
    else:
        path = config.PATH

    start = time.time()

    # window_switch_train
    try:
        ct = crocotime_api.Crocotime(**config.CT_PARAMS)

        employees = ct.get_employees()
        departments = flat_departments(ct.get_departments(), employees)

        a = time.time()
        write_file(os.path.join(path, "departments.csv"),
                   departments,
                   ['department_id',
                    'parent_id',
                    'display_name',
                    'is_enabled',
                    ])
        print('departments_inserted ', len(departments), ' ', time.time() - a)

        # выгружаем сотрудников
        a = time.time()
        write_file(os.path.join(path, "employees.csv"),
                   employees,
                   ['employee_id',
                    'display_name',
                    'first_name',
                    'second_name',
                    'email',
                    'privilege',
                    'time_zone',
                    'parent_group_id',
                    'is_enabled',
                    ])
        print('employees_inserted ', len(employees), ' ', time.time() - a)

        # записываем в список id сотрудников для запроса employee_activity и work_periods
        employee_ids = [employee['employee_id'] for employee in employees]

        # выгружаем данные по каждому интервалу
        program_ids = set()
        window_ids = set()

        # write file head
        wst_fieldnames = [
            'employee_id',
            'interval_ms_begin',
            'interval_ms_end',
            'interval_begin',
            'interval_end',
            'computer_id',
            'program_id',
            'window_id',
            'url',
        ]
        write_file(os.path.join(path, "window_switch_train.csv"), None, wst_fieldnames)

        # Если надо поделить активность по секундам
        if config.SPLIT_PER_SECONDS:
            write_file(os.path.join(path, "window_switch_train_per_sec.csv"), None, wst_fieldnames)

        # выгрузка интервалами
        for interval in range(time_begin, time_end, config.INTERVAL):
            print(time.time())

            interval_begin = interval
            if interval + config.INTERVAL - 1 > time_end:
                interval_end = time_end
            else:
                interval_end = interval + config.INTERVAL - 1

            # чтобы отображать прогресс загрузки
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(interval_begin)),
                  time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(interval_end)))

            # делаем список запросов к api_window_switch_train по каждому сотруднику
            window_switch_trains_requests = [(interval_begin, interval_end, employee['employee_id'])
                                             for employee in employees]  # if employee['parent_group_id'] in [2, 3]]

            a = time.time()
            with Pool(processes=config.PROCESSES) as pool:
                results = pool.starmap(ct.get_window_switch_train, window_switch_trains_requests)
            print('Pool ', time.time() - a, '\n')

            # полученный результат превращаем из json в список
            window_switch_train = [dict(
                track,
                interval_ms_begin=track['interval'][0],
                interval_ms_end=track['interval'][1],
                interval_begin=time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.gmtime(track["interval"][0])),
                interval_end=time.strftime("%Y-%m-%d %H:%M:%S",
                                           time.gmtime(track["interval"][1])),
            )
                for res in results for track in res]

            append_to_file(os.path.join(path, "window_switch_train.csv"), window_switch_train, wst_fieldnames)

            # Если надо поделить активность по секундам
            if config.SPLIT_PER_SECONDS:
                for track in window_switch_train:
                    wst_per_seconds = [dict(
                        employee_id=track['employee_id'],
                        interval_ms_begin=x,
                        interval_ms_end=x + 1,
                        interval_begin=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(x)),
                        interval_end=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(x + 1)),
                        computer_id=track['computer_id'],
                        program_id=track['program_id'],
                        window_id=track['window_id'],
                        url=track.get('url', ''),
                    ) for x in range(track['interval_ms_begin'], track['interval_ms_end'], 1)]
                    append_to_file(os.path.join(path, "window_switch_train_per_sec.csv"), wst_per_seconds, wst_fieldnames)

            for track in window_switch_train:
                program_ids.add(track['program_id'])
                window_ids.add(track['window_id'])

            print('wst_inserted ', len(window_switch_train), ' ', time.time() - a)

        # programs
        programs = ct.get_programs(list(program_ids))
        a = time.time()
        write_file(os.path.join(path, "programs.csv"), programs, ['program_id', 'name', 'program_group_id'])
        print('programs_inserted ', len(programs), ' ', time.time() - a)

        # windows
        windows = ct.get_windows(list(window_ids))
        a = time.time()
        write_file(os.path.join(path, "windows.csv"), windows, ['window_id', 'title'])
        #             window["title"].encode('utf8', 'ignore').decode('utf8', 'ignore')
        print('windows_inserted ', len(windows), ' ', time.time() - a)

        # program_groups
        program_groups_ids = set(program['program_group_id'] for program in programs)
        program_groups = ct.get_program_groups(list(program_groups_ids))
        a = time.time()
        write_file(os.path.join(path, "program_groups.csv"),
                   program_groups,
                   ['program_group_id',
                    'parent_group_id',
                    'display_name',
                    ])
        print('progr_gr_inserted ', len(program_groups), ' ', time.time() - a)

        print('window_switch_train + progr_gr + programs + windows DONE')  # для отладки
        # window_switch_train end

        # выгружаем активнссть сотрудников и приход/уход по дням
        activities_fieldnames = [
            'employee_id',
            'first_name',
            'second_name',
            'permitted_time',
            'forbidden_time',
            'unknown_time',
            'late_count',
            'early_end_count',
            'absenteeism',
            'late_time',
            'early_end_time',
            'work_day_count',
            'summary_time',
            'norm',
            'schedule_day_count',
            'date_begin',
            'date_end',
        ]
        write_file(os.path.join(path, "employee_activity.csv"), None, activities_fieldnames)

        ewp_fieldnames = [
            'employee_id',
            'first_name',
            'second_name',
            'day',
            'begin',
            'end',
            'date',
        ]
        write_file(os.path.join(path, "employee_work_periods.csv"), None, ewp_fieldnames)

        ms_in_day = 24 * 60 * 60
        for day in range(time_begin, time_end, ms_in_day):
            day_begin = day
            if day + ms_in_day - 1 > time_end:
                day_end = time_end
            else:
                day_end = day + ms_in_day - 1

            employee_activities_raw = ct.get_employee_activity(day_begin, day_end, employee_ids)
            employee_activities = [dict(activity,
                                        date_begin=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(day_begin)),
                                        date_end=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(day_end)),
                                        )
                                   for activity in employee_activities_raw]
            a = time.time()
            append_to_file(os.path.join(path, "employee_activity.csv"), employee_activities, activities_fieldnames)
            print('employee_activities_inserted ', len(employee_activities), ' ', time.time() - a)

            employee_work_periods_raw = ct.get_employees_work_periods(day_begin, employee_ids)
            employee_work_periods = [dict(period,
                                          date=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(day_begin)),
                                          )
                                     for period in employee_work_periods_raw]
            a = time.time()
            append_to_file(os.path.join(path, "employee_work_periods.csv"), employee_work_periods, ewp_fieldnames)
            print('employee_work_periods_inserted ', len(employee_work_periods), ' ', time.time() - a)

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

    end = time.time()
    print('\nTotal time: ', end - start)
    print("Результат лежит в ", path)


if __name__ == '__main__':
    load()
