from datetime import datetime


class Storage:
    def __init__(self, file):
        self.filename = file
        create = open(self.filename, "a+")  # Ensure file is created and not erased
        create.close()


class LineageTreeStorage(Storage):
    def write(self, json):
        with open(self.filename, "w") as file:
            file.write(json)
        return self


class NextCladeLineagesStorage(Storage):
    lineages = set()

    def read(self):
        with open(self.filename, "r") as file:
            for lineage in file:
                self.lineages.add(lineage.strip())
        return self.lineages

    def write(self, lineages):
        with open(self.filename, "w") as file:
            for lineage in lineages:
                file.write(f"{lineage}\n")
        return self


class LastUpdatedStorage(Storage):
    _date_format = "%Y-%m-%d %H:%M:%S"

    def read(self):
        with open(self.filename, "r") as file:
            line = file.readline().strip()
            if not line:
                return None, None
            return line.split(", ")

    def write(self, url):
        with open(self.filename, "w") as file:
            time = datetime.now().strftime(self._date_format)
            file.write(f"{time}, {url}\n")
        return self
