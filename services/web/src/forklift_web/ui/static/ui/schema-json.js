/* JSON for the schema editor, keeping what JSON.parse loses: key order and number text.

   A JavaScript object lists keys that look like integers ("2024") first, and JSON.parse turns
   10.0 into 10. Both matter here: a file without a header row gets its column names from the
   order of "properties", and a document should be saved as it was written. parse() returns
   objects as JSONObject (ordered entries) and numbers as JSONNumber (their text), and records
   where each key and value is in the text; print() writes a value as JSON.stringify(value,
   null, 2) would, and as the gateway shows saved versions.

   contextAt() reads the text before the cursor, valid or not, and tells completion where it
   is: the path of keys and indexes, and whether a key or a value goes there. */
(function () {
  "use strict";

  function JSONObject(entries) {
    this.entries = entries || []; // [key, value] pairs, in order
    this.spans = []; // per entry: {key: [from, to], value: [from, to]} when parsed
  }

  JSONObject.prototype.index = function (key) {
    for (var i = this.entries.length - 1; i >= 0; i--) {
      if (this.entries[i][0] === key) return i;
    }
    return -1;
  };
  JSONObject.prototype.has = function (key) {
    return this.index(key) >= 0;
  };
  JSONObject.prototype.get = function (key) {
    var i = this.index(key);
    return i < 0 ? undefined : this.entries[i][1];
  };
  JSONObject.prototype.keys = function () {
    return this.entries.map(function (entry) {
      return entry[0];
    });
  };
  /* Replaces the value in place; a new key goes after the key ``after`` (null: first), or
     last. */
  JSONObject.prototype.set = function (key, value, after) {
    var i = this.index(key);
    if (i >= 0) {
      this.entries[i][1] = value;
      return;
    }
    var at = this.entries.length;
    if (after === null) at = 0;
    else if (after !== undefined && this.has(after)) at = this.index(after) + 1;
    this.entries.splice(at, 0, [key, value]);
  };
  JSONObject.prototype.remove = function (key) {
    var i = this.index(key);
    if (i >= 0) this.entries.splice(i, 1);
  };
  JSONObject.prototype.rename = function (from, to) {
    var i = this.index(from);
    if (i >= 0) this.entries[i][0] = to;
  };

  function JSONNumber(text) {
    this.text = text;
  }

  JSONNumber.prototype.valueOf = function () {
    return Number(this.text);
  };

  function ParseError(message, from, to) {
    this.message = message;
    this.from = from;
    this.to = to === undefined ? from + 1 : to;
  }

  // ------------------------------------------------------------------ parse

  var NUMBER = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  var LITERALS = { true: true, false: false, null: null };
  var ESCAPES = { '"': '"', "\\": "\\", "/": "/", b: "\b", f: "\f", n: "\n", r: "\r", t: "\t" };

  function Parser(text) {
    this.text = text;
    this.at = 0;
    this.duplicates = []; // [from, to, key]
  }

  Parser.prototype.fail = function (message, from, to) {
    throw new ParseError("Not valid JSON: " + message, from === undefined ? this.at : from, to);
  };

  Parser.prototype.space = function () {
    var text = this.text;
    while (this.at < text.length && " \t\n\r".indexOf(text[this.at]) >= 0) this.at++;
  };

  Parser.prototype.value = function () {
    this.space();
    var text = this.text;
    var ch = text[this.at];
    if (ch === "{") return this.object();
    if (ch === "[") return this.array();
    if (ch === '"') return this.string();
    if (ch === "-" || (ch >= "0" && ch <= "9")) return this.number();
    var word = /^[A-Za-z]+/.exec(text.slice(this.at, this.at + 12));
    if (word && Object.prototype.hasOwnProperty.call(LITERALS, word[0])) {
      this.at += word[0].length;
      return LITERALS[word[0]];
    }
    if (ch === undefined) this.fail("the text ends where a value should be.");
    if (word) {
      this.fail(
        "“" + word[0] + "” is not a JSON value; text goes in double quotes.",
        this.at,
        this.at + word[0].length
      );
    }
    this.fail("expected a value (a string, number, object, array, true, false or null).");
  };

  Parser.prototype.number = function () {
    NUMBER.lastIndex = this.at;
    var match = NUMBER.exec(this.text);
    if (!match || !match[0] || match[0] === "-") this.fail("this is not a number.");
    this.at += match[0].length;
    return new JSONNumber(match[0]);
  };

  Parser.prototype.string = function () {
    var text = this.text;
    var start = this.at++;
    var out = "";
    for (;;) {
      var ch = text[this.at];
      if (ch === undefined) this.fail("this text has no closing double quote.", start);
      if (ch === '"') break;
      if (ch < " ") {
        this.fail("a line break or tab inside a string must be written as \\n or \\t.");
      }
      if (ch === "\\") {
        var next = text[this.at + 1];
        if (next === "u" && /^[0-9a-fA-F]{4}$/.test(text.slice(this.at + 2, this.at + 6))) {
          out += String.fromCharCode(parseInt(text.slice(this.at + 2, this.at + 6), 16));
          this.at += 6;
          continue;
        }
        if (!Object.prototype.hasOwnProperty.call(ESCAPES, next)) {
          this.fail(
            "“\\" + (next || "") + "” is not an escape JSON knows; write \\\\ for a backslash.",
            this.at,
            this.at + 2
          );
        }
        out += ESCAPES[next];
        this.at += 2;
        continue;
      }
      out += ch;
      this.at++;
    }
    this.at++;
    return out;
  };

  Parser.prototype.object = function () {
    var object = new JSONObject();
    var start = this.at++;
    var seen = {};
    this.space();
    if (this.text[this.at] === "}") {
      this.at++;
      return this.close(object, start);
    }
    for (;;) {
      this.space();
      if (this.text[this.at] !== '"') {
        if (this.at >= this.text.length)
          this.fail("the object that starts here is not closed with }.", start);
        this.fail("expected a key in double quotes.");
      }
      var keyFrom = this.at;
      var key = this.string();
      var keyTo = this.at;
      this.space();
      if (this.text[this.at] !== ":") this.fail("expected : after the key “" + key + "”.");
      this.at++;
      this.space();
      var valueFrom = this.at;
      var value = this.value();
      var span = { key: [keyFrom, keyTo], value: [valueFrom, this.at] };
      if (Object.prototype.hasOwnProperty.call(seen, key)) {
        // Like Python's json.loads: the first key's place, the last key's value
        this.duplicates.push([keyFrom, keyTo, key]);
        var index = object.index(key);
        object.entries[index][1] = value;
        object.spans[index] = span;
      } else {
        seen[key] = true;
        object.entries.push([key, value]);
        object.spans.push(span);
      }
      this.space();
      var ch = this.text[this.at];
      this.at++;
      if (ch === "}") return this.close(object, start);
      if (ch !== ",") {
        if (ch === undefined) this.fail("the object that starts here is not closed with }.", start);
        this.fail("expected , or } after the value of “" + key + "”.", this.at - 1);
      }
    }
  };

  Parser.prototype.array = function () {
    var array = [];
    var start = this.at++;
    array.spans = [];
    this.space();
    if (this.text[this.at] === "]") {
      this.at++;
      return this.close(array, start);
    }
    for (;;) {
      this.space();
      var from = this.at;
      array.push(this.value());
      array.spans.push([from, this.at]);
      this.space();
      var ch = this.text[this.at];
      this.at++;
      if (ch === "]") return this.close(array, start);
      if (ch !== ",") {
        if (ch === undefined) this.fail("the list that starts here is not closed with ].", start);
        this.fail("expected , or ] after this item.", this.at - 1);
      }
    }
  };

  Parser.prototype.close = function (container, start) {
    container.span = [start, this.at];
    return container;
  };

  function parseWithDuplicates(text) {
    var parser = new Parser(text);
    var value = parser.value();
    parser.space();
    if (parser.at < text.length) parser.fail("there is more text after the end of the document.");
    return { value: value, duplicates: parser.duplicates };
  }

  /* The value of ``text``; throws ParseError (with .from and .to) when it is not JSON. */
  function parse(text) {
    return parseWithDuplicates(text).value;
  }

  /* What is wrong with ``text`` as JSON, as diagnostics: one error, or a warning per key
     that appears twice in an object (the gateway keeps the last one). */
  function problems(text) {
    try {
      return parseWithDuplicates(text).duplicates.map(function (found) {
        return {
          from: found[0],
          to: found[1],
          severity: "warning",
          message: "“" + found[2] + "” appears twice in this object; only the last one is kept.",
        };
      });
    } catch (error) {
      if (!(error instanceof ParseError)) throw error;
      var to = Math.min(error.to, text.length);
      return [
        { from: Math.min(error.from, to), to: to, severity: "error", message: error.message },
      ];
    }
  }

  // ------------------------------------------------------------------ print

  function print(value, indent) {
    indent = indent || "";
    var inner = indent + "  ";
    if (value instanceof JSONObject) {
      if (!value.entries.length) return "{}";
      return (
        "{\n" +
        value.entries
          .map(function (entry) {
            return inner + JSON.stringify(entry[0]) + ": " + print(entry[1], inner);
          })
          .join(",\n") +
        "\n" +
        indent +
        "}"
      );
    }
    if (Array.isArray(value)) {
      if (!value.length) return "[]";
      return (
        "[\n" +
        value
          .map(function (item) {
            return inner + print(item, inner);
          })
          .join(",\n") +
        "\n" +
        indent +
        "]"
      );
    }
    if (value instanceof JSONNumber) return value.text;
    return JSON.stringify(value);
  }

  // ------------------------------------------------------------------ places in the text

  /* Where ``path`` (keys and indexes from the root) is in ``root``, a value parse() returned
     for ``text``: the value's span, or for a path that goes further than the document, the
     place of the deepest part that exists. */
  function locate(root, path) {
    var node = root;
    var place = { from: root.span ? root.span[0] : 0, to: root.span ? root.span[0] + 1 : 0 };
    for (var i = 0; i < path.length; i++) {
      var part = path[i];
      var span = null;
      if (node instanceof JSONObject && typeof part === "string") {
        var index = node.index(part);
        if (index >= 0) {
          span = node.spans[index];
          node = node.entries[index][1];
        }
      } else if (Array.isArray(node) && typeof part === "number" && part < node.length) {
        span = { value: node.spans[part] };
        node = node[part];
      }
      if (!span) break;
      var big = node instanceof JSONObject || Array.isArray(node);
      // An object or list as a whole is marked by its key (or opening bracket), not all its lines
      if (big && span.key) place = { from: span.key[0], to: span.key[1] };
      else if (big) place = { from: span.value[0], to: span.value[0] + 1 };
      else place = { from: span.value[0], to: span.value[1] };
    }
    return place;
  }

  var WORD = /[^\s,:{}\[\]"]/;

  /* Where the cursor at ``offset`` is: {path, slot, from, quoted, keys}. ``slot`` is "key"
     (a key of the object at ``path`` goes here), "value" (the value at ``path``), or null;
     ``from`` is where the text typed so far starts (an opening quote included) and ``keys``
     the keys the object already has before the cursor. */
  function contextAt(text, offset) {
    var stack = [];
    var i = 0;
    var token = null; // the string or word the cursor is in: [from, quoted]

    function top() {
      return stack[stack.length - 1];
    }

    function childPath() {
      var frame = top();
      if (!frame) return [];
      return frame.path.concat([frame.type === "object" ? frame.key : frame.index]);
    }

    function done() {
      // A value (or a key) ended: what the container expects next
      var frame = top();
      if (!frame) return;
      if (frame.type === "object") frame.state = frame.state === "key" ? "colon" : "comma";
      else frame.state = "comma";
    }

    while (i < offset) {
      var ch = text[i];
      if (ch === '"') {
        var start = i++;
        var value = "";
        while (i < offset && text[i] !== '"') {
          value += text[i] === "\\" ? text[++i] || "" : text[i];
          i++;
        }
        if (i >= offset) {
          token = [start, true];
          break;
        }
        i++;
        var frame = top();
        if (frame && frame.type === "object" && frame.state === "key") {
          frame.key = value;
          frame.keys.push(value);
        }
        done();
        continue;
      }
      if (ch === "{" || ch === "[") {
        stack.push({
          type: ch === "{" ? "object" : "array",
          path: childPath(),
          state: ch === "{" ? "key" : "value",
          key: null,
          keys: [],
          index: 0,
        });
      } else if (ch === "}" || ch === "]") {
        stack.pop();
        done();
      } else if (ch === ":") {
        if (top() && top().state === "colon") top().state = "value";
      } else if (ch === ",") {
        var current = top();
        if (current && current.type === "object") current.state = "key";
        else if (current) {
          current.index++;
          current.state = "value";
        }
      } else if (WORD.test(ch)) {
        var begin = i;
        while (i < offset && WORD.test(text[i])) i++;
        if (i >= offset) {
          token = [begin, false];
          break;
        }
        done();
        continue;
      }
      i++;
    }

    var frame = top();
    var context = {
      path: [],
      slot: null,
      from: token ? token[0] : offset,
      quoted: !!(token && token[1]),
      keys: [],
    };
    if (!frame) return context;
    if (frame.type === "object" && frame.state === "key") {
      context.slot = "key";
      context.path = frame.path;
      context.keys = frame.keys;
    } else if (frame.state === "value") {
      context.slot = "value";
      context.path = childPath();
    }
    return context;
  }

  window.ForkliftJSON = {
    JSONObject: JSONObject,
    JSONNumber: JSONNumber,
    ParseError: ParseError,
    parse: parse,
    problems: problems,
    print: print,
    locate: locate,
    contextAt: contextAt,
  };
})();
