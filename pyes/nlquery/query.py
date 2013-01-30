import re
import time
from datetime import datetime

from pyes.query import *
from pyes.filters import *
from pyes.utils import ESRange
from pyparsing import ParseResults
from parsedatetime.parsedatetime import Calendar

from .exceptions import ParsingError
from .parser import LuceneParser


WILDCARD = re.compile('(?P<text>(\\w|[-])*[*?](\\w|[-*?])*)')
AND, OR, NOT = "AND", "OR", "NOT"
CALENDAR = Calendar()


def parseDate(value):
    return value
    tpl, flags = CALENDAR.parse(value)
    if flags == 0:
        # Didn't parse as a date. Just return the orig value
        return None if value == '*' else value
    tpl = time.gmtime(time.mktime(tpl))
    result = datetime(*tpl[:7]).isoformat()
    print "%s became %s" % (value, result)
    return result


def floatOrTimestamp(value):
    """
    Try to make value a float, otherwise parse as a date.
    """
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return parseDate(value)


def _flatten(parsed):
    if not parsed.asDict() and len(parsed) == 1:
        return parsed[0]
    return parsed


def _build_query(parsed, firstrun=True):
    """
    Analyze a C{ParseResults} and determine how to querify it
    """
    if not isinstance(parsed, ParseResults):
        return parsed

    parsed = _flatten(parsed)
    filter_ = None

    if 'or_' in parsed or 'and_' in parsed:
        usebool = any('required' in q or 'prohibit' in q for q in parsed 
                      if q not in ('OR', 'AND'))
        if usebool:
            filt = BoolFilter()
            for q in (x for x in parsed if x not in ('OR', 'AND')):
                if 'required' in q:
                    filt.add_must(_build_query(q[1], False))
                elif 'prohibit' in q or 'not_' in q:
                    filt.add_must_not(_build_query(q[1], False))
                else:
                    filt.add_should(_build_query(q, False))
        else:
            filters = []
            for q in (x for x in parsed if x not in ('OR', 'AND')):
                filters.append(_build_query(q, False))
            filt = ANDFilter(filters) if 'and_' in parsed else ORFilter(filters)
        filter_ = filt
    elif 'required' in parsed:
        # Already taken care of by bool query
        filter_ = _build_query(parsed[1], False)
    elif 'prohibit' in parsed or 'not_' in parsed:
        # Already taken care of by bool query
        filter_ = NotFilter(_build_query(parsed[1], False))
    elif 'query' in parsed:
        if parsed.field == '_ids':
            values = parsed.query.split(',')
            filter_ = IdsFilter(values)
        elif 'is_contains' in parsed:
            values = parsed.query.split(',')
            filter_ = TermsFilter(parsed.field or '_all', values)
        elif WILDCARD.match(parsed.query):
            boost = parsed.boost or 1.0
            if (parsed.query.count('*')==1 and '?' not in parsed.query and
                parsed.query.endswith('*')):
                filter_ = QueryFilter(TextQuery(parsed.field or '_all',
                                       parsed.query[:-1], 'phrase_prefix'))
            else:
                filter_ = QueryFilter(WildcardQuery(parsed.field or '_all',
                                                    parsed.query, boost=boost))
        else:
            filter_ = QueryFilter(TextQuery(parsed.field or '_all', parsed.query))
            #filter_ = TermFilter(parsed.field or '_all', parsed.query)
    elif 'phrase' in parsed:
        filter_ = QueryFilter(TextQuery(parsed.field or '_all', parsed.phrase, 'phrase'))
    elif 'range' in parsed:
        range_ = parsed['range']
        filter_ = RangeFilter(ESRange(
            parsed.field or '_all',
            floatOrTimestamp(range_.lower.strip()),
            floatOrTimestamp(range_.upper.strip()),
            bool(range_.incl_lower),
            bool(range_.incl_upper)
        ))
    elif 'subquery' in parsed:
        filter_ = _build_query(parsed.subquery, False)

    if firstrun:
        return FilteredQuery(MatchAllQuery(), filter_)
    else:
        return filter_


def compile_query(query):
    """
    Compile a DSL query to an L{ElasticQuery}.
    """
    parsed = LuceneParser.parseString(query)
    result = _build_query(parsed)
    return result

