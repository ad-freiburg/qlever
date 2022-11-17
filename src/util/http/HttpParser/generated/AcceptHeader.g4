grammar AcceptHeader;
/*
SEE https://datatracker.ietf.org/doc/html/rfc7231#section-5.3.2
*/

/*
Accept = #( media-range [ accept-params ] )
*/
accept: rangeAndParams (OWS* ',' OWS* rangeAndParams)*;
acceptWithEof : accept EOF;

/* extra rule for the accept rule for easier parsing */
rangeAndParams: mediaRange acceptParams?;

/*
(no space between the star and the slash, but this confuses antlr)
 media-range    = ( "* /*"
                      / ( type "/" "*" )
                      / ( type "/" subtype )
                      ) *( OWS* ";" OWS* parameter )
*/
mediaRange: (MediaRangeAll | (type '/' '*') | (type '/' subtype)) (OWS* ';' OWS* parameter)*;
MediaRangeAll: Star '/' Star;

type: token;
subtype:token;

/* accept-params  = weight *( accept-ext )
weight = OWS ";" OWS "q=" qvalue
qvalue = ( "0" [ "." *3DIGIT ] ) / ( "1" [ "." *3"0" ] )
*/
acceptParams: weight (acceptExt)*;
weight: OWS* ';' OWS* QandEqual qvalue;
qvalue: DIGIT ( Dot DIGIT*)?; /* TODO in parser: max 3 decimal digits, and must be <= 1.0*/

QandEqual: 'q' '=';

/* accept-ext = OWS* ";" OWS* token [ "=" ( token / quoted-string ) ] */
acceptExt: OWS* ';' OWS* token ('=' (token | quotedString))?;

/* parameter = token "=" ( token / quoted-string ) */
parameter: token '=' (token | quotedString);

/* token = 1*tchar */
token:  tchar+;
/*
 tchar          = "!" / "#" / "$" / "%" / "&" / "'" / "*"
                     / "+" / "-" / "." / "^" / "_" / "`" / "|" / "~"
                     / DIGIT / ALPHA
                     ; any VCHAR, except delimiters
*/
tchar:
	  ExclamationMark
	| Hashtag
	| DollarSign
	| Percent
	| Ampersand
	| SQuote
	| Star
	| Plus
    | Minus
	| Dot
	| Caret
    | Underscore
	| BackQuote
	| VBar
	| Tilde
	| DIGIT
	| ALPHA;

DIGIT : [0-9];
ALPHA: [A-Za-z];

OWS: (SP | HTAB);

Minus :'-';
Dot   : '.';
Underscore: '_';
Tilde : '~';
QuestionMark :'?';
Slash :'/';
ExclamationMark: '!';
Colon:':';
At: '@';
DollarSign:'$';
Hashtag:'#';
Ampersand:'&';
Percent:'%';
SQuote:'\'';
Star:'*';
Plus:'+';
Caret:'^';
BackQuote:'`';
VBar:'|';

/*
 A string of text is parsed as a single value if it is quoted using
   double-quote marks.

     quoted-string  = DQUOTE *( qdtext / quoted-pair ) DQUOTE
     qdtext         = HTAB / SP /%x21 / %x23-5B / %x5D-7E / obs-text
     obs-text       = %x80-FF

   Comments can be included in some HTTP header fields by surrounding
   the comment text with parentheses.  Comments are only allowed in
   fields containing "comment" as part of their field value definition.

     comment        = "(" *( ctext / quoted-pair / comment ) ")"
     ctext          = HTAB / SP / %x21-27 / %x2A-5B / %x5D-7E / obs-text

   The backslash octet ("\") can be used as a single-octet quoting
   mechanism within quoted-string and comment constructs.  Recipients
   that process the value of a quoted-string MUST handle a quoted-pair
   as if it were replaced by the octet following the backslash.

     quoted-pair    = "\" ( HTAB / SP / VCHAR / obs-text )
*/

quotedString: DQUOTE (QDTEXT | quoted_pair)* DQUOTE;
QDTEXT: HTAB | SP | '\u0021' | '\u0027' .. '\u005b' | '\u005d' .. '\u007e' | OBS_TEXT;
OBS_TEXT: '\u0080' ..'\u00ff';
quoted_pair: '\\' (HTAB | SP | VCHAR | OBS_TEXT);
DQUOTE: '"';


/*
https://datatracker.ietf.org/doc/html/rfc5234#appendix-B.1
 SP =  %x20
 VCHAR          =  %x21-7E
 */
 SP: '\u0020';
 HTAB: '\u0009';
 VCHAR: '\u0021' .. '\u007e';

