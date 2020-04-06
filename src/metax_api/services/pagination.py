from rest_framework.pagination import LimitOffsetPagination


class DirectoryPagination(LimitOffsetPagination):

    page_size = 10
    page_size_query_param = 'page_size'

    def paginate_queryset(self, queryset, request, view=None):
        self.count = self.get_count(queryset)
        self.limit = self.get_limit(request)
        if self.limit is None:
            return None

        self.offset = self.get_offset(request)
        self.request = request
        if self.count > self.limit and self.template is not None:
            self.display_page_controls = True

        if self.count == 0 or self.offset > self.count:
            return []

        # serves filters directories_only and files_only which returnes dictionaries
        if len(queryset) == 1:
            key = list(queryset.keys())[0]
            return dict({key: queryset[key][self.offset:self.offset + self.limit]})

        dirs = []
        files = []
        dir_len = len(queryset['directories'])

        # if no directories left to show
        if self.offset >= dir_len:
            offset = self.offset - dir_len
            files = queryset['files'][offset:offset + self.limit]

        # if directories are not enough for one page limit
        elif (self.offset + self.limit) >= dir_len:
            dirs = queryset['directories'][self.offset:]
            files_to_show = self.limit - (dir_len - self.offset)
            if files_to_show > 0:
                files = queryset['files'][0:files_to_show]

        # if enough directories for page limit
        else:
            dirs = queryset['directories'][self.offset:self.offset + self.limit]

        return dict({'directories': dirs, 'files': files})

    def get_count(self, queryset):
        """
        Determine a count of directory dictionary.
        """
        count = 0
        for q, v in queryset.items():
            count = count + len(v)
        return count
