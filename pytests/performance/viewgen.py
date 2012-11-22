class ViewGen:

    DDOC_NAMES = ["A", "B", "C", "D", "E", "F", "G", "H"]

    VIEW_NAMES = ['city1', 'city2', 'realm1', 'experts1', 'experts2', 'realm2',
                  'realm3', 'category']

    MAP_FUNCTIONS = [
        """
        function(doc, meta) {
            if (doc.city != null) {
                emit(doc.city, null);
            }
        }
        """,
        """
        function(doc, meta) {
            if (doc.city != null) {
                emit(doc.city, ["Name:" + doc.name, "E-mail:" + doc.email]);
            }
        }
        """,
        """
        function(doc, meta) {
            if (doc.realm != null) {
                emit(doc.realm, null);
            }
        }
        """,
        """
        function(doc, meta) {
            if (doc.category == 2) {
                emit([doc.name, doc.coins], null);
            }
        }
        """,
        """
        function(doc, meta) {
            emit([doc.category, doc.coins], null);
        }
        """,
        """
        function(doc, meta) {
            emit([doc.realm, doc.coins], null)
        }
        """,
        """
        function(doc, meta) {
            emit([doc.realm, doc.coins], [meta.id, doc.name, doc.email]);
        }
        """,
        """
        function(doc, meta) {
            emit([doc.category, doc.realm, doc.coins], [meta.id, doc.name, doc.email]);
        }
        """,
        """
        function (doc, meta) {
            if (doc.achievements.length > 0) {
                emit(doc.category, doc.coins);
            }
        }
        """]

    REDUCE_FUNCTIONS = ["_count", "_sum", "_stats"]

    def generate_ddocs(self, pattern=None, add_reduce=False):
        """Generate dictionary with design documents and views.
        Pattern looks like:
            [8, 8, 8] -- 8 ddocs (8 views, 8 views, 8 views)
            [2, 2, 4] -- 3 ddocs (2 views, 2 views, 4 views)
            [8] -- 1 ddoc (8 views)
            [1, 1, 1, 1, 1, 1, 1, 1] -- 8 ddocs (1 view)
        If `add_reduce` argument is True, additional ddoc with single
        map-reduce view is added
        """

        ddocs = dict()

        index_of_map = 0
        index_of_ddoc = 0

        for number_of_views in pattern:
            ddoc_name = self.DDOC_NAMES[index_of_ddoc]
            ddocs[ddoc_name] = {'views': {}}
            for index_of_view in range(number_of_views):
                try:
                    view_name = self.VIEW_NAMES[index_of_map]
                except IndexError:
                    index_of_map = 0
                    view_name = self.VIEW_NAMES[index_of_map]
                ddocs[ddoc_name]['views'][view_name] = {}
                ddocs[ddoc_name]['views'][view_name]['map'] = \
                    self.MAP_FUNCTIONS[index_of_map]
                index_of_map += 1
            index_of_ddoc += 1

        if add_reduce:
            ddocs['reduce'] = {
                'views': {
                    'reduce': {
                        'map': self.MAP_FUNCTIONS[-1],
                        'reduce': self.REDUCE_FUNCTIONS[-1]
                    }
                }
            }

        return ddocs

    def generate_all_docs_view(self):
        """ Return view definition which mimics primary index (aka _all_docs).
        """

        MAP_FUNCTION = """
            function (doc, meta) {
                emit(meta.id, {"rev": meta.rev});
            }"""

        return {'all': {'views': {'docs': {'map': MAP_FUNCTION}}}}

    def generate_queries(self, limit, query_suffix, ddocs, bucket='default',
                         use_all_docs=False, use_reduce=False, pseudo=False,
                         extend=False):
        """Generate string from permuted queries.

        Optional arguments:
        use_all_docs -- add query on primary index
        use_reduce -- add query on view with reduce step
        pseudo -- only queries on pseudo "all docs" index

        if ddocs is None it returns only queries on primary index
        """

        # Base path
        b = '/{0}/'.format(bucket)

        # Only all_docs case
        if ddocs is None:
            queries_by_kind = \
                [[b + '_all_docs?limit=' + str(limit) + '&startkey="{key}"']]
            remaining = [1]
            queries = self.compute_queries(queries_by_kind, remaining,
                                           query_suffix)
            return self.join_queries(queries)

        # Pseudo all docs case
        if pseudo:
            queries_by_kind =\
                [[b + '_design/all/_view/docs?limit=' + str(limit) + '&startkey="{key}"']]
            remaining = [1]
            queries = self.compute_queries(queries_by_kind, remaining,
                                           query_suffix)
            return self.join_queries(queries)

        # General case
        ddoc_names =\
            [name for name, ddoc in sorted(ddocs.iteritems()) for view in ddoc["views"]]

        q = {'city': b + '_design/' + ddoc_names[0] +
                     '/_view/city1?limit=' + str(limit) + '&startkey="{city}"',
             'city2': b + '_design/' + ddoc_names[1] +
                      '/_view/city2?limit=' + str(limit) + '&startkey="{city}"',
             'realm': b + '_design/' + ddoc_names[2] +
                      '/_view/realm1?limit=30&startkey="{realm}"',
             'experts': b + '_design/' + ddoc_names[3] +
                        '/_view/experts1?limit=30&startkey="{name}"',
             'coins-beg': b + '_design/' + ddoc_names[4] +
                          '/_view/experts2?limit=30&startkey=[0,{int10}]&endkey=[0,{int100}]',
             'coins-exp': b + '_design/' + ddoc_names[4] +
                          '/_view/experts2?limit=30&startkey=[2,{int10}]&endkey=[2,{int100}]',
             'and0': b + '_design/' + ddoc_names[5] +
                     '/_view/realm2?limit=30&startkey=["{realm}",{coins}]',
             'and1': b + '_design/' + ddoc_names[6] +
                     '/_view/realm3?limit=30&startkey=["{realm}",{coins}]',
             'and2': b + '_design/' + ddoc_names[7] +
                     '/_view/category?limit=30&startkey=[0,"{realm}",{coins}]'}

        queries_by_kind = [
            [  # 45% / 5 = 9
               q['city'],
               q['city2'],
               q['realm'],
               q['experts']],
            [  # 30% / 5 = 6
               q['coins-beg'],
               q['coins-exp']],
            [  # 25% / 5 = 5
               q['and0'],
               q['and1'],
               q['and2']]]

        remaining = [9, 6, 5]

        if use_all_docs:
            q['all_docs'] = b + '_all_docs?limit=' + str(limit) +\
                            '&startkey="{key}"'
            queries_by_kind = [[q['all_docs']]] + queries_by_kind
            remaining = [5] + remaining

        if use_reduce:
            q['reduce'] = b + '_design/reduce/_view/reduce?limit=' + str(limit)
            queries_by_kind = queries_by_kind + [[q['reduce']]]
            remaining = remaining + [5]

        queries = self.compute_queries(queries_by_kind, remaining,
                                       query_suffix)
        if extend:
            queries = self.extend_queries(queries, ddocs)
        queries = self.join_queries(queries)

        return queries

    def compute_queries(self, queries_by_kind, remaining, suffix=""):
        """Return a list of permuted queries"""
        i = 0
        queries = []

        while remaining.count(0) < len(remaining):
            kind = i % len(remaining)
            count = remaining[kind]
            if count > 0:
                remaining[kind] = count - 1
                k = queries_by_kind[kind]
                queries.append(k[count % len(k)] + suffix)
            i += 1

        return queries

    def extend_queries(self, queries, ddocs):
        """Extend number of queries if number of views is more than 8. It only
        makes sense when originally there were only queries on single design
        document ([8] pattern). Otherwise it's better to avoid this method.
        """

        rename = lambda query, name: query.replace('/A/', '/{0}/'.format(name))

        ddoc_names = [ddoc_name for ddoc_name in sorted(ddocs.keys())]

        return [rename(query, name) for query in queries for name in ddoc_names]

    def join_queries(self, queries):
        """Join queries into string"""
        queries = ';'.join(queries)
        queries = queries.replace('[', '%5B')
        queries = queries.replace(']', '%5D')
        queries = queries.replace(',', '%2C')
        return queries
