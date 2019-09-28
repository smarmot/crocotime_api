import csv


def write_file(filename, data, fieldnames):
    with open(filename, "w", encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames, lineterminator="\n", delimiter=';', extrasaction='ignore')

        writer.writeheader()
        if data:
            for row in data:
                writer.writerow(row)


def append_to_file(filename, data, fieldnames):
    with open(filename, "a", encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames, lineterminator="\n", delimiter=';', extrasaction='ignore')

        for row in data:
            writer.writerow(row)
