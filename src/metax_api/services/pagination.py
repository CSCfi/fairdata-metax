from rest_framework.pagination import LimitOffsetPagination


class DirectoryPagination(LimitOffsetPagination):

    page_size = 10
    page_size_query_param = "page_size"

    def paginate_directory_data(self, dirs, files, request, view=None):
        """
        Takes in directories and files as lists or querysets.
        Output is list tuple.
        """
        self.count = self.get_count([dirs, files])

        self.request = request

        self.limit = self.get_limit(self.request)
        if self.limit is None:
            return None

        self.offset = self.get_offset(self.request)

        if self.count > self.limit and self.template is not None:
            self.display_page_controls = True

        if self.count == 0 or self.offset > self.count:
            return [], []

        if dirs:
            dir_len = len(dirs) if isinstance(dirs, list) else dirs.count()
        else:
            dir_len = 0

        # if no directories left to show
        if self.offset >= dir_len:
            if dirs:
                dirs = []
            offset = self.offset - dir_len
            if files:
                files = files[offset : offset + self.limit]

        # if directories are not enough for one page limit
        elif (self.offset + self.limit) > dir_len:
            dirs = dirs[self.offset :]
            if files:
                files_to_show = self.limit - (dir_len - self.offset)
                if files_to_show > 0:
                    files = files[0:files_to_show]

        # if enough directories for page limit
        else:
            dirs = dirs[self.offset : self.offset + self.limit]
            if files:
                files = []

        return dirs, files

    def get_count(self, contents):
        """
        Determine a count of directory dictionary.
        Accepts lists and querysets.
        """
        count = 0

        for content in contents:
            if content:
                count = (
                    count + len(content) if isinstance(content, list) else count + content.count()
                )

        return count
