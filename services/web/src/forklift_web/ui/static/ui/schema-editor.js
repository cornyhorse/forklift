/* The schema editor: CodeMirror over the document's text area, with completion from the schema
   standards, live diagnostics, a Columns view (schema-form.js) and the starting points.

   - The text area stays the form field. The editor writes every change back to it, so saving,
     checking against a file and the page without JavaScript all read the same text.
   - The editor lives in a shadow root. CodeMirror adds its styles there as constructed style
     sheets, which the Content-Security-Policy (styles only from the gateway's files) allows;
     the <style> elements it would add to the page itself are refused.
   - Diagnostics: JSON syntax at once, from ForkliftJSON; after a pause, the gateway's review of
     the draft (POST /schemas/check/, the code saving uses), placed at the JSON path of each
     problem. They are listed under the editor too, and the status line (aria-live) sums up.
   - Completion (Ctrl+Space, or as you type a key or value) offers what the standards in
     schema-standards/ use at that place: keys, type and format values, the x-* extensions and
     their keys, and the document's column names where a column belongs.
   - Starting points: a schema generated from an upload replaces the draft when asked (undo
     brings the draft back); Ctrl+Enter checks the draft against the file chosen beside it. */
(function () {
  "use strict";

  var CM = window.CodeMirror;
  var J = window.ForkliftJSON;
  var Form = window.ForkliftSchemaForm;
  var REVIEW_DELAY = 400;
  var AUTO_CHECK_DELAY = 2500;
  var editor = null; // the page's editor, for the buttons HTMX swaps in

  if (!CM || !J || !Form) return; // the text area stays the editor

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function plural(count, word) {
    return count + " " + word + (count === 1 ? "" : "s");
  }

  function has(object, key) {
    return Object.prototype.hasOwnProperty.call(object, key);
  }

  // ------------------------------------------------------------------ look

  var reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var dark = window.matchMedia("(prefers-color-scheme: dark)").matches;

  // Colours are forklift.css's custom properties, which reach into the shadow root.
  var theme = CM.EditorView.theme(
    {
      "&": {
        color: "var(--fg)",
        backgroundColor: "var(--bg)",
        border: "1px solid var(--muted)",
        borderRadius: "var(--radius)",
        fontSize: "0.875rem",
        maxHeight: "75vh",
      },
      "&.cm-focused": { outline: "3px solid var(--focus)", outlineOffset: "2px" },
      ".cm-scroller": { fontFamily: "var(--mono)", lineHeight: "1.45", overflow: "auto" },
      ".cm-content, .cm-gutter": { minHeight: "24rem" },
      ".cm-content": { caretColor: "var(--fg)" },
      ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--fg)" },
      ".cm-gutters": {
        backgroundColor: "var(--surface)",
        color: "var(--muted)",
        borderRight: "1px solid var(--border)",
      },
      ".cm-activeLine": { backgroundColor: "var(--surface)" },
      ".cm-activeLineGutter": { backgroundColor: "var(--surface-2)", color: "var(--fg)" },
      ".cm-selectionBackground, &.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground":
        { backgroundColor: "var(--selection)" },
      ".cm-matchingBracket, &.cm-focused .cm-matchingBracket": {
        backgroundColor: "var(--surface-2)",
        outline: "1px solid var(--muted)",
      },
      ".cm-tooltip": {
        backgroundColor: "var(--surface)",
        color: "var(--fg)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius)",
      },
      ".cm-tooltip.cm-tooltip-autocomplete > ul": { fontFamily: "var(--mono)", maxHeight: "16em" },
      ".cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]": {
        backgroundColor: "var(--accent)",
        color: "var(--accent-fg)",
      },
      ".cm-completionDetail": { fontStyle: "normal", opacity: "0.85" },
      ".cm-completionInfo": {
        maxWidth: "24rem",
        padding: "0.4rem 0.6rem",
        fontFamily: "var(--sans)",
      },
      ".cm-diagnostic": { padding: "0.3rem 0.6rem", fontFamily: "var(--sans)" },
      ".cm-diagnostic-error": { borderLeft: "4px solid var(--danger)" },
      ".cm-diagnostic-warning": { borderLeft: "4px solid var(--warn)" },
      ".cm-lint-marker": { width: "0.9em", height: "0.9em" },
    },
    { dark: dark }
  );

  var highlight = CM.HighlightStyle.define([
    { tag: CM.tags.propertyName, color: "var(--syntax-key)" },
    { tag: CM.tags.string, color: "var(--syntax-string)" },
    { tag: CM.tags.number, color: "var(--syntax-number)" },
    { tag: [CM.tags.bool, CM.tags.null], color: "var(--syntax-literal)" },
  ]);

  // ------------------------------------------------------------------ completion

  /* The vocabulary node for ``path`` and the node it is an item or entry of. */
  function lookup(root, path) {
    var node = root;
    var parent = null;
    for (var i = 0; i < path.length && node; i++) {
      var part = path[i];
      parent = node;
      if (typeof part === "number") node = node.items || null;
      else node = (node.keys && has(node.keys, part) && node.keys[part]) || node.columnKeys || null;
    }
    return { node: node, parent: parent };
  }

  /* The end of the token being completed: past the rest of a quoted string (and its quote). */
  function tokenEnd(doc, to, quoted) {
    if (!quoted) return to;
    var line = doc.lineAt(to);
    var rest = doc.sliceString(to, line.to);
    var match = /^(?:[^"\\]|\\.)*"/.exec(rest);
    return match ? to + match[0].length : to;
  }

  function applyText(insert, quoted, isKey) {
    return function (view, completion, from, to) {
      var doc = view.state.doc;
      var start = quoted ? from - 1 : from;
      var end = tokenEnd(doc, to, quoted);
      var text = insert;
      if (isKey && !/^\s*:/.test(doc.sliceString(end, end + 20))) text += ": ";
      view.dispatch({
        changes: { from: start, to: end, insert: text },
        selection: { anchor: start + text.length },
        userEvent: "input.complete",
      });
    };
  }

  function keyOptions(found, here, columns) {
    var node = found.node;
    if (!node) return [];
    var taken = here.keys;
    var options = Object.keys(node.keys || {}).map(function (key) {
      var info = node.keys[key];
      return {
        label: key,
        type: "property",
        detail: info.in ? "in " + info.in.replace(/ /g, ", ") : undefined,
        info: info.doc,
        boost: key.indexOf("x-") === 0 ? -1 : 0,
        apply: applyText(JSON.stringify(key), here.quoted, true),
      };
    });
    // Keys that name a column (as in x-transformations.column_transformations), not new columns
    var newColumn = here.path.length === 1 && here.path[0] === "properties";
    if (node.columnKeys && !newColumn) {
      columns.forEach(function (name) {
        if (!(node.keys && has(node.keys, name))) {
          options.push({
            label: name,
            type: "variable",
            detail: "column",
            boost: 1,
            apply: applyText(JSON.stringify(name), here.quoted, true),
          });
        }
      });
    }
    return options.filter(function (option) {
      return taken.indexOf(option.label) < 0;
    });
  }

  function valueOptions(found, here, columns) {
    var node = found.node;
    var parent = found.parent;
    var inList = !!(parent && parent.items && node === parent.items);
    var options = [];
    function add(label, insert, type, detail) {
      options.push({
        label: label,
        type: type,
        detail: detail,
        apply: applyText(insert, here.quoted, false),
      });
    }
    if (inList && parent.columnNames) {
      columns.forEach(function (name) {
        add(name, JSON.stringify(name), "variable", "column");
      });
    }
    var values = (node && node.values) || (inList && parent.values) || [];
    values.forEach(function (value) {
      add(value, JSON.stringify(value), "enum", "in the standards");
    });
    var kinds = (node && node.kinds) || [];
    if (kinds.indexOf("boolean") >= 0 && !here.quoted) {
      add("true", "true", "keyword");
      add("false", "false", "keyword");
    }
    return options;
  }

  function completionSource(state) {
    return function (context) {
      if (!state.vocabulary) return null;
      var before = context.state.doc.sliceString(0, context.pos);
      var here = J.contextAt(before, context.pos);
      if (!here.slot) return null;
      var from = here.quoted ? here.from + 1 : here.from;
      if (!context.explicit && from === context.pos && !here.quoted) return null;
      var found = lookup(state.vocabulary.root, here.path);
      var options =
        here.slot === "key"
          ? keyOptions(found, here, state.columns)
          : valueOptions(found, here, state.columns);
      if (!options.length) return null;
      return { from: from, options: options, validFor: /^[\w$.\-]*$/ };
    };
  }

  // ------------------------------------------------------------------ diagnostics

  function reviewer(url) {
    var pending = null;
    var last = { text: null, problems: null };
    return function (text) {
      if (text === last.text) return Promise.resolve(last.problems);
      if (pending) pending.abort();
      pending = new AbortController();
      return fetch(url, {
        method: "POST",
        body: new URLSearchParams({ document: text }),
        headers: { Accept: "application/json", "X-CSRFToken": window.Forklift.csrfToken() },
        credentials: "same-origin",
        signal: pending.signal,
      }).then(function (response) {
        var json = (response.headers.get("Content-Type") || "").indexOf("application/json") === 0;
        if (!response.ok || !json) {
          throw new Error(
            response.ok
              ? "the answer was not the review (were you signed out? reload the page)"
              : "the gateway answered " + response.status
          );
        }
        return response.json().then(function (body) {
          last = { text: text, problems: body.problems };
          return body.problems;
        });
      });
    };
  }

  function diagnostic(value, problem) {
    var place = J.locate(value, problem.path);
    return {
      from: place.from,
      to: place.to,
      severity: problem.blocking ? "error" : "warning",
      message: problem.message,
    };
  }

  function linter(state) {
    return function (view) {
      var text = view.state.doc.toString();
      if (!text.trim()) {
        state.report(view, { empty: true, diagnostics: [] });
        return [];
      }
      var local = J.problems(text);
      if (local.length && local[0].severity === "error") {
        state.report(view, { diagnostics: local });
        return local;
      }
      var value = J.parse(text);
      var properties = value instanceof J.JSONObject ? value.get("properties") : null;
      state.columns = properties instanceof J.JSONObject ? properties.keys() : [];
      return state
        .review(text)
        .then(
          function (problems) {
            return { diagnostics: local.concat(problems.map(diagnostic.bind(null, value))) };
          },
          function (error) {
            return {
              diagnostics: local,
              unavailable: error.name === "AbortError" ? null : error.message,
            };
          }
        )
        .then(function (result) {
          result.valid = true;
          state.report(view, result);
          return result.diagnostics;
        });
    };
  }

  // ------------------------------------------------------------------ the editor

  /* Replaces the editor's text with ``text``, changing only the part that differs. */
  function replaceText(view, text, userEvent) {
    var old = view.state.doc.toString();
    if (old === text) return;
    var start = 0;
    var end = old.length;
    var newEnd = text.length;
    while (start < end && start < newEnd && old[start] === text[start]) start++;
    while (end > start && newEnd > start && old[end - 1] === text[newEnd - 1]) {
      end--;
      newEnd--;
    }
    view.dispatch({
      changes: { from: start, to: end, insert: text.slice(start, newEnd) },
      userEvent: userEvent,
    });
  }

  function where(view, offset) {
    var line = view.state.doc.lineAt(Math.min(offset, view.state.doc.length));
    return "line " + line.number + ", column " + (offset - line.from + 1);
  }

  function pretty(text) {
    return J.print(J.parse(text));
  }

  function initEditor(container) {
    if (container.dataset.ready) return;
    container.dataset.ready = "1";
    var textarea = container.querySelector("textarea[data-json-editor]");
    var label = container.querySelector('label[for="' + textarea.id + '"]');
    var status = document.getElementById(textarea.id + "-status");
    var problemList = container.querySelector("[data-problems]");
    var format = container.querySelector("[data-format-json]");
    var switcher = container.querySelector(".view-switch");
    var columnsView = container.querySelector("[data-columns-view]");
    var validate = document.getElementById("validate-button");
    var auto = document.getElementById("auto-validate");
    var host = element("div", "code-editor");
    textarea.after(host);
    var shadow = host.attachShadow({ mode: "open" });
    var mount = element("div");
    shadow.appendChild(mount);

    var autoTimer = null;
    var lastChecked = null;
    var state = {
      vocabulary: null,
      columns: [],
      review: reviewer(container.dataset.checkUrl),
      report: report,
    };
    var form = null;

    function showProblems(view, diagnostics) {
      problemList.textContent = "";
      problemList.hidden = !diagnostics.length;
      diagnostics.forEach(function (found) {
        var item = element("li", found.severity);
        var go = element("button", "link", where(view, found.from));
        go.type = "button";
        go.addEventListener("click", function () {
          showView("json");
          var length = view.state.doc.length;
          view.dispatch({
            selection: { anchor: Math.min(found.from, length), head: Math.min(found.to, length) },
            scrollIntoView: true,
          });
          view.focus();
        });
        item.appendChild(go);
        item.appendChild(document.createTextNode(": " + found.message));
        problemList.appendChild(item);
      });
    }

    function report(view, result) {
      var diagnostics = result.diagnostics;
      var message;
      status.classList.remove("ok", "warn", "error");
      if (result.empty) {
        message =
          "Empty: type or paste a JSON object" +
          (document.getElementById("generation") ? ", or start from a sample." : ".");
      } else if (!result.valid) {
        message = diagnostics[0].message.replace(
          /^Not valid JSON: /,
          "Not valid JSON yet (" + where(view, diagnostics[0].from) + "): "
        );
        status.classList.add("error");
      } else {
        message = "Valid JSON: " + plural(state.columns.length, "column") + " in “properties”.";
        if (result.unavailable)
          message += " The gateway could not review it: " + result.unavailable + ".";
        else if (diagnostics.length)
          message += " " + plural(diagnostics.length, "problem") + " to look at, listed below.";
        else message += " No problems found.";
        var blocking = diagnostics.some(function (found) {
          return found.severity === "error";
        });
        status.classList.add(
          blocking ? "error" : diagnostics.length || result.unavailable ? "warn" : "ok"
        );
        scheduleCheck(view);
      }
      if (status.textContent !== message) status.textContent = message;
      showProblems(view, diagnostics);
    }

    function scheduleCheck(view) {
      clearTimeout(autoTimer);
      var text = view.state.doc.toString();
      if (!auto || !auto.checked || !validate || text === lastChecked) return;
      autoTimer = setTimeout(function () {
        lastChecked = text;
        validate.click();
      }, AUTO_CHECK_DELAY);
    }

    function checkNow() {
      if (!validate) return false;
      validate.click();
      return true;
    }

    var view = new CM.EditorView({
      doc: textarea.value,
      parent: mount,
      root: shadow,
      extensions: [
        CM.lineNumbers(),
        CM.highlightActiveLineGutter(),
        CM.highlightSpecialChars(),
        CM.history(),
        CM.drawSelection({ cursorBlinkRate: reducedMotion ? 0 : 1200 }),
        CM.indentOnInput(),
        CM.bracketMatching(),
        CM.closeBrackets(),
        CM.highlightActiveLine(),
        CM.json(),
        CM.syntaxHighlighting(highlight),
        CM.autocompletion({ override: [completionSource(state)] }),
        CM.linter(linter(state), { delay: REVIEW_DELAY }),
        CM.lintGutter(),
        CM.Prec.high(CM.keymap.of([{ key: "Mod-Enter", run: checkNow }])),
        // No Tab binding: Tab and Shift+Tab move focus, so the editor is no keyboard trap
        CM.keymap.of(
          [].concat(
            CM.closeBracketsKeymap,
            CM.defaultKeymap,
            CM.historyKeymap,
            CM.completionKeymap,
            CM.lintKeymap
          )
        ),
        CM.EditorView.contentAttributes.of({
          "aria-label": label.textContent.replace(/\s+/g, " ").trim(),
        }),
        CM.EditorView.updateListener.of(function (update) {
          if (!update.docChanged) return;
          textarea.value = update.state.doc.toString();
          if (!columnsView.hidden) form.render(textarea.value, where.bind(null, view));
        }),
        theme,
      ],
    });
    textarea.hidden = true;
    label.addEventListener("click", function (event) {
      event.preventDefault();
      if (columnsView.hidden) view.focus();
    });
    container.querySelector("[data-editor-keys]").hidden = false;
    // The text area is the form field: it has the editor's text at every moment.
    textarea.form.addEventListener("submit", function () {
      textarea.value = view.state.doc.toString();
    });

    form = Form.create(columnsView, {
      vocabulary: function () {
        return state.vocabulary;
      },
      onChange: function (text) {
        replaceText(view, text, "input.form");
      },
    });

    function showView(name) {
      var columns = name === "columns";
      switcher.querySelectorAll("[data-view]").forEach(function (button) {
        button.setAttribute("aria-pressed", String(button.dataset.view === name));
      });
      host.hidden = columns;
      format.hidden = columns;
      columnsView.hidden = !columns;
      if (columns) form.render(view.state.doc.toString(), where.bind(null, view));
    }

    switcher.hidden = false;
    switcher.addEventListener("click", function (event) {
      var button = event.target.closest("[data-view]");
      if (button) showView(button.dataset.view);
    });

    format.hidden = false;
    format.addEventListener("click", function () {
      try {
        replaceText(view, pretty(view.state.doc.toString()), "input.format");
      } catch (error) {
        if (!(error instanceof J.ParseError)) throw error;
        view.dispatch({
          selection: { anchor: Math.min(error.from, view.state.doc.length) },
          scrollIntoView: true,
        });
      }
      view.focus();
    });

    fetch(container.dataset.vocabulary, { credentials: "same-origin" })
      .then(function (response) {
        return response.ok ? response.json() : null;
      })
      .then(function (vocabulary) {
        state.vocabulary = vocabulary;
      })
      .catch(function () {
        state.vocabulary = null; // no completion; everything else works
      });

    editor = {
      view: view,
      load: function (downloadUrl, statusNode) {
        statusNode.textContent = "Loading the generated schema…";
        return window.Forklift.artifactText(downloadUrl)
          .then(function (text) {
            replaceText(view, pretty(text), "input.generated");
            statusNode.textContent =
              "The draft is now the generated schema; Ctrl+Z in the JSON view brings the earlier draft back.";
          })
          .catch(function (error) {
            statusNode.textContent = "The generated schema could not be loaded: " + error.message;
          });
      },
    };

    if (textarea.dataset.loadArtifact && !textarea.value.trim()) {
      editor.load(textarea.dataset.loadArtifact, status);
    }
  }

  function initUseGenerated(button) {
    if (button.dataset.ready) return;
    button.dataset.ready = "1";
    button.hidden = false;
    button.addEventListener("click", function () {
      var statusNode = button.closest(".card").querySelector("[data-generated-status]");
      if (editor) editor.load(button.dataset.useGenerated, statusNode);
    });
  }

  function init(root) {
    root.querySelectorAll("[data-schema-editor]").forEach(initEditor);
    root.querySelectorAll("[data-use-generated]").forEach(initUseGenerated);
  }

  if (window.htmx) {
    window.htmx.onLoad(init);
  } else {
    document.addEventListener("DOMContentLoaded", function () {
      init(document);
    });
  }
})();
