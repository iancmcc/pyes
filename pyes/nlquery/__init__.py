from pyelasticsearch import ElasticSearch as BaseElasticSearch

from .query import compile_query


__all__ = ["ElasticSearch"]


class ElasticSearch(BaseElasticSearch):

    def _search_dsl(self, query, *args, **kwargs):
        query = compileQuery(query)
        return self.search_json(query, *args, **kwargs)

    def _search_json(self, query, *args, **kwargs):
        return BaseElasticSearch.search(self, query, *args, **kwargs)

    def search(self, query, *args, **kwargs):
        functor = self._search_json
        # Probably better to do this the other way around (try DSL first), but
        # the parser doesn't exist yet so this is easier
        # TODO: Swap these once the parser is written
        try:
            json.loads(query)
        except ValueError:
            # Probably DSL
            functor = self._search_dsl
        return functor(query, *args, **kwargs)

