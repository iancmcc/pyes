#
# lucene_grammar.py
#
# Copyright 2011, Paul McGuire
#
# implementation of Lucene grammar, as decribed
# at http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/docs/queryparsersyntax.html
#

from pyparsing import (Literal, CaselessKeyword, Forward, Regex, QuotedString, Suppress,
    Optional, Group, FollowedBy, operatorPrecedence, opAssoc, ParseException,
                       ParserElement, SkipTo)
ParserElement.enablePackrat()

COLON,LBRACK,RBRACK,LBRACE,RBRACE,TILDE,CARAT = map(Literal,":[]{}~^")
LPAR,RPAR = map(Suppress,"()")
and_ = CaselessKeyword("AND")
or_ = CaselessKeyword("OR")
not_ = CaselessKeyword("NOT")
to_ = CaselessKeyword("TO")
keyword = and_ | or_ | not_

expression = Forward()

valid_word = Regex(r'([a-zA-Z0-9*_+/.,\?-]|\\[!(){}\[\]^"~*?\\:])+').setName("word")
valid_word.setParseAction(
    lambda t : t[0].replace('\\\\',chr(127)).replace('\\','').replace(chr(127),'\\').lower()
)

string = QuotedString('"')

required_modifier = Literal("+")("required")
prohibit_modifier = Literal("-")("prohibit")
integer = Regex(r"\d+").setParseAction(lambda t:int(t[0]))
proximity_modifier = Group(TILDE + integer("proximity"))
number = Regex(r'\d+(\.\d+)?').setParseAction(lambda t:float(t[0]))
fuzzy_modifier = TILDE + Optional(number, default=0.5)("fuzzy")

term = Forward()
field_name = valid_word.copy().setName("fieldname")
range_search = Group((LBRACK('incl_lower') | LBRACE('excl_lower')) +
                     SkipTo(to_)('lower') + to_ + 
                     SkipTo(RBRACK | RBRACE)("upper") +
                     (RBRACK('incl_upper') | RBRACE('excl_upper')))
boost = (CARAT + number("boost"))

string_expr = Group(string + proximity_modifier) | string
word_expr = (Group(valid_word + fuzzy_modifier) | valid_word)
term << (Optional(field_name("field") + COLON + Optional(COLON("is_contains"))) + 
         (word_expr("query") | string_expr("phrase") | range_search("range") | Group(LPAR + expression + RPAR)("subquery")) + Optional(boost))
term.setParseAction(lambda t:[t] if 'field' in t or 'query' in t or 'boost' in t else t)
    
expression << operatorPrecedence(term,
    [
    (required_modifier | prohibit_modifier, 1, opAssoc.RIGHT),
    ((not_ | '!')("not_").setParseAction(lambda:"NOT"), 1, opAssoc.RIGHT),
    (Optional(and_ | '&&')("and_").setParseAction(lambda:"AND"), 2, opAssoc.LEFT),
    ((or_ | '||')("or_").setParseAction(lambda:"OR"), 2, opAssoc.LEFT),
    ])

LuceneParser = expression


