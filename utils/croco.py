def flat_departments(departments, employees):
    """Flat departments hierarchy in lists of employees and departments
    """

    departments_flat = []

    def _flat(items, parent_id=-1):
        for item in items:
            if 'department_id' in item:
                departments_flat.append({
                    'department_id': item['department_id'],
                    'display_name': item['display_name'],
                    'is_enabled': item['is_enabled'],
                    'parent_id': parent_id,
                })
                if 'items' in item:
                    _flat(item['items'], item['department_id'])
            if 'employee_id' in item:
                next(d.update({'is_enabled': item['is_enabled']}) for d in employees
                     if d.get('employee_id') == item['employee_id'])

    _flat(departments)
    return departments_flat
