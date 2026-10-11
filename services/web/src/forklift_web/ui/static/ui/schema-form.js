/* The Columns view of the schema editor: the common parts of a schema as a form, one fieldset
   per entry of "properties", in their order (a file without a header row gets its column names
   from it).

   The form edits the document parsed by ForkliftJSON and hands the printed JSON back to the
   editor at once; everything it does not show is kept as it was. Per column it edits the name
   (renaming it in "required", "x-primaryKey", "x-uniqueConstraints" and "x-transformations"
   too), type and nullability, format, x-special-type, description, required, primary key,
   unique (a one-column x-uniqueConstraints entry), the rules JSON Schema puts on a column
   (minimum, maximum, minLength, maxLength, pattern, enum) and which transformation steps are
   on. Columns are added, removed and moved with buttons; focus stays on the control used. */
(function () {
  "use strict";

  var J = window.ForkliftJSON;
  var TYPES = [
    ["string", "Text (string)"],
    ["integer", "Whole number (integer)"],
    ["number", "Number"],
    ["boolean", "True or false (boolean)"],
    ["array", "List (array)"],
    ["object", "Object"],
  ];
  // The number rules a type offers: [key, label, whole numbers only]
  var BOUNDS = {
    integer: [
      ["minimum", "Smallest value", false],
      ["maximum", "Largest value", false],
    ],
    string: [
      ["minLength", "Shortest text (characters)", true],
      ["maxLength", "Longest text (characters)", true],
    ],
  };
  BOUNDS.number = BOUNDS.integer;
  var NUMBER = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;
  var COUNT = /^(?:0|[1-9]\d*)$/;
  var HEAD_KEYS = ["$schema", "$id", "title", "description", "type"];

  function element(tag, attrs, text) {
    var node = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (name) {
      if (attrs[name] !== false) node.setAttribute(name, attrs[name] === true ? "" : attrs[name]);
    });
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function object(value) {
    return value instanceof J.JSONObject ? value : null;
  }

  function text(owner, key) {
    var value = owner.get(key);
    return typeof value === "string" ? value : "";
  }

  // ------------------------------------------------------------------ the document

  /* The key of ``keys`` that comes last in ``owner`` (new keys go after it), if any. */
  function afterLast(owner, keys) {
    var found;
    keys.forEach(function (key) {
      if (owner.has(key) && (found === undefined || owner.index(key) > owner.index(found))) {
        found = key;
      }
    });
    return found;
  }

  function childObject(owner, key, after) {
    if (!object(owner.get(key))) owner.set(key, new J.JSONObject(), after);
    return owner.get(key);
  }

  /* Adds ``name`` to the list ``owner[key]`` (made after ``after`` if missing) or removes it. */
  function setMember(owner, key, name, on, after) {
    var items = owner.get(key);
    if (!Array.isArray(items)) {
      if (!on) return;
      items = [];
      owner.set(key, items, after);
    }
    var at = items.indexOf(name);
    if (on && at < 0) items.push(name);
    if (!on && at >= 0) items.splice(at, 1);
  }

  function replaceIn(items, from, to) {
    for (var i = 0; Array.isArray(items) && i < items.length; i++) {
      if (items[i] === from) items[i] = to;
    }
  }

  /* Sets a key, or removes it for ""; a new key goes after ``after`` (null: first). */
  function setKey(owner, key, value, after) {
    if (value === "") owner.remove(key);
    else owner.set(key, value, after);
  }

  /* {base, nullable} for a type the form can edit; null for unions and the like. */
  function typeOf(definition) {
    if (definition.has("anyOf") || definition.has("oneOf")) return null;
    var declared = definition.get("type");
    if (declared === undefined) return { base: "", nullable: false };
    if (typeof declared === "string" && declared !== "null") {
      return { base: declared, nullable: false };
    }
    if (Array.isArray(declared) && declared.length === 2 && declared.indexOf("null") >= 0) {
      var base = declared[1 - declared.indexOf("null")];
      if (typeof base === "string" && base !== "null") return { base: base, nullable: true };
    }
    return null;
  }

  function Schema(doc) {
    this.doc = doc;
  }

  Schema.prototype.columns = function () {
    return object(this.doc.get("properties"));
  };

  Schema.prototype.primary = function () {
    var columns =
      object(this.doc.get("x-primaryKey")) && this.doc.get("x-primaryKey").get("columns");
    return Array.isArray(columns) ? columns : [];
  };

  Schema.prototype.unique = function () {
    var all = this.doc.get("x-uniqueConstraints");
    return Array.isArray(all) ? all : [];
  };

  /* x-transformations.column_transformations, made when ``create`` is set; else null. */
  Schema.prototype.transformations = function (create) {
    var section = object(this.doc.get("x-transformations"));
    if (!create) return section && object(section.get("column_transformations"));
    return childObject(childObject(this.doc, "x-transformations"), "column_transformations");
  };

  Schema.prototype.setPrimary = function (name, on) {
    if (!on && !this.primary().length) return;
    var after = afterLast(this.doc, ["properties", "required"]);
    var key = childObject(this.doc, "x-primaryKey", after);
    setMember(key, "columns", name, on, null);
    var columns = key.get("columns");
    if (!columns.length) this.doc.remove("x-primaryKey");
    // "type" (single or composite) must match the number of columns, when it is given
    else if (key.has("type")) key.set("type", columns.length > 1 ? "composite" : "single");
  };

  function columnsOf(constraint) {
    var columns = object(constraint) && constraint.get("columns");
    return Array.isArray(columns) ? columns : [];
  }

  function alone(constraint, name) {
    var columns = columnsOf(constraint);
    return columns.length === 1 && columns[0] === name;
  }

  Schema.prototype.isUnique = function (name) {
    return this.unique().some(function (constraint) {
      return alone(constraint, name);
    });
  };

  Schema.prototype.setUnique = function (name, on) {
    var all = this.doc.get("x-uniqueConstraints");
    if (on) {
      if (!Array.isArray(all)) {
        all = [];
        var after = afterLast(this.doc, ["properties", "required", "x-primaryKey"]);
        this.doc.set("x-uniqueConstraints", all, after);
      }
      all.push(
        new J.JSONObject([
          ["name", name + "_unique"],
          ["columns", [name]],
        ])
      );
    } else if (Array.isArray(all)) {
      for (var i = all.length - 1; i >= 0; i--) if (alone(all[i], name)) all.splice(i, 1);
      if (!all.length) this.doc.remove("x-uniqueConstraints");
    }
  };

  /* The constraints with this column and others, as "name (a, b)". */
  Schema.prototype.sharedUnique = function (name) {
    return this.unique()
      .filter(function (constraint) {
        var columns = columnsOf(constraint);
        return columns.length > 1 && columns.indexOf(name) >= 0;
      })
      .map(function (constraint) {
        var label = text(constraint, "name") || "unnamed";
        return label + " (" + columnsOf(constraint).join(", ") + ")";
      });
  };

  Schema.prototype.rename = function (from, to) {
    this.columns().rename(from, to);
    replaceIn(this.doc.get("required"), from, to);
    replaceIn(this.primary(), from, to);
    this.unique().forEach(function (constraint) {
      replaceIn(columnsOf(constraint), from, to);
    });
    var steps = this.transformations(false);
    if (steps) steps.rename(from, to);
  };

  Schema.prototype.removeColumn = function (name) {
    this.columns().remove(name);
    setMember(this.doc, "required", name, false);
    this.setPrimary(name, false);
    this.setUnique(name, false);
    var steps = this.transformations(false);
    if (steps) steps.remove(name);
  };

  Schema.prototype.addColumn = function () {
    if (!this.columns())
      this.doc.set("properties", new J.JSONObject(), afterLast(this.doc, HEAD_KEYS));
    var columns = this.columns();
    var n = columns.entries.length + 1;
    while (columns.has("column_" + n)) n++;
    columns.set("column_" + n, new J.JSONObject([["type", "string"]]));
  };

  Schema.prototype.move = function (from, to) {
    var entries = this.columns().entries;
    entries.splice(to, 0, entries.splice(from, 1)[0]);
  };

  // ------------------------------------------------------------------ the vocabulary

  function columnKeyValues(vocabulary, key) {
    var properties = vocabulary && vocabulary.root.keys.properties;
    var node = properties && properties.columnKeys.keys[key];
    return ((node && node.values) || []).map(function (value) {
      return [value, value];
    });
  }

  function stepNames(vocabulary) {
    var section = vocabulary && vocabulary.root.keys["x-transformations"];
    var steps = section && section.keys && section.keys.column_transformations;
    return steps && steps.columnKeys ? Object.keys(steps.columnKeys.keys || {}) : [];
  }

  // ------------------------------------------------------------------ the form

  function create(container, options) {
    var schema = null;
    var printed = null; // the text the form's document was read from or written as
    var open = {}; // "rules:<name>" / "steps:<name>" -> true for the open <details>
    var list = element("ol", { class: "columns" });
    var note = element("p", { class: "muted" });
    var add = element("button", { type: "button", class: "secondary", "data-control": "add" });
    var status = element("p", { class: "visually-hidden", "aria-live": "polite" });
    var actions = element("div", { class: "actions" });
    add.textContent = "Add a column";
    actions.appendChild(add);
    container.appendChild(
      element(
        "p",
        { class: "muted" },
        "Changes here go into the JSON at once (Ctrl+Z in the JSON view undoes them); " +
          "everything this form does not show stays as it is."
      )
    );
    [note, list, actions, status].forEach(function (node) {
      container.appendChild(node);
    });

    function commit() {
      printed = J.print(schema.doc);
      options.onChange(printed);
    }

    /* Applies ``fn`` to the schema, redraws the form and focuses ``control`` of the column at
       ``index`` (or the Add button), announcing ``message``. */
    function change(fn, index, control, message) {
      fn(schema);
      commit();
      draw();
      var scope = list.querySelector('[data-column="' + index + '"]');
      var target = scope && scope.querySelector('[data-control="' + control + '"]');
      (target || add).focus();
      status.textContent = message;
    }

    // ---------------------------------------------------------------- controls

    function field(id, label, input, wide) {
      var wrap = element("div", { class: wide ? "field wide" : "field" });
      wrap.appendChild(element("label", { for: id }, label));
      input.id = id;
      wrap.appendChild(input);
      return wrap;
    }

    function select(choices, current, control) {
      var node = element("select", { "data-control": control });
      var known = choices.some(function (choice) {
        return choice[0] === current;
      });
      // A value no standard lists is kept, and shown as it is
      choices.concat(known ? [] : [[current, current]]).forEach(function (choice) {
        var option = element("option", { value: choice[0] }, choice[1]);
        option.selected = choice[0] === current;
        node.appendChild(option);
      });
      return node;
    }

    function checkbox(id, label, checked, control, onChange) {
      var wrap = element("div", { class: "check" });
      var input = element("input", { type: "checkbox", id: id, "data-control": control });
      input.checked = checked;
      input.addEventListener("change", function () {
        onChange(input.checked);
        commit();
      });
      wrap.appendChild(input);
      wrap.appendChild(element("label", { for: id }, label));
      return wrap;
    }

    /* A text box whose value goes through ``apply`` when it changes; ``apply`` returns the
       message to show when the value cannot be used. */
    function textInput(value, control, apply, attrs) {
      var input = element("input", { type: "text", autocomplete: "off", "data-control": control });
      Object.keys(attrs || {}).forEach(function (name) {
        input.setAttribute(name, attrs[name]);
      });
      input.value = value;
      input.addEventListener("change", function () {
        var problem = apply(input.value);
        var error = input.parentNode && input.parentNode.querySelector(".errorlist");
        if (error) error.remove();
        input.setAttribute("aria-invalid", problem ? "true" : "false");
        input.removeAttribute("aria-describedby");
        if (problem) {
          error = element("p", { class: "errorlist", id: input.id + "-error" }, problem);
          input.parentNode.appendChild(error);
          input.setAttribute("aria-describedby", error.id);
        }
      });
      return input;
    }

    /* Writes a text box's value to ``owner[key]`` ("" removes the key). */
    function writer(owner, key, after) {
      return function (value) {
        setKey(owner, key, value, after);
        commit();
        return null;
      };
    }

    function numberWriter(definition, key, whole) {
      return function (value) {
        value = value.trim();
        if (value && !(whole ? COUNT : NUMBER).test(value)) {
          return whole ? "A whole number, 0 or more." : "A number, such as 0 or 2.5.";
        }
        return writer(definition, key)(value && new J.JSONNumber(value));
      };
    }

    function details(kind, name, summary) {
      var node = element("details", { "data-open": kind + ":" + name });
      node.open = !!open[kind + ":" + name];
      node.appendChild(element("summary", {}, summary));
      return node;
    }

    // ---------------------------------------------------------------- one column

    function rules(name, id, definition, type) {
      var section = details("rules", name, "Rules");
      var grid = element("div", { class: "row2" });
      (BOUNDS[type.base] || []).forEach(function (rule) {
        var value = definition.get(rule[0]);
        var shown = value instanceof J.JSONNumber ? value.text : "";
        var input = textInput(shown, rule[0], numberWriter(definition, rule[0], rule[2]), {
          inputmode: rule[2] ? "numeric" : "decimal",
        });
        grid.appendChild(field(id + rule[0], rule[1], input));
      });
      if (type.base === "string") {
        var pattern = textInput(
          text(definition, "pattern"),
          "pattern",
          writer(definition, "pattern"),
          {
            spellcheck: "false",
            class: "code",
          }
        );
        grid.appendChild(field(id + "pattern", "Pattern (a regular expression)", pattern, true));
        grid.appendChild(allowedValues(definition, id));
      }
      if (!grid.children.length) {
        section.appendChild(
          element(
            "p",
            { class: "muted" },
            "The form has no rules for this type; the JSON view has."
          )
        );
      }
      section.appendChild(grid);
      return section;
    }

    function allowedValues(definition, id) {
      var values = definition.get("enum");
      var strings =
        values === undefined ||
        (Array.isArray(values) &&
          values.every(function (value) {
            return typeof value === "string";
          }));
      var area = element("textarea", { rows: 3, spellcheck: "false", "data-control": "enum" });
      area.disabled = !strings;
      area.value = strings && values ? values.join("\n") : "";
      area.addEventListener("change", function () {
        var lines = area.value.split("\n").filter(Boolean);
        writer(definition, "enum")(lines.length ? lines : "");
      });
      var wrap = field(id + "enum", "Allowed values (one per line; none: any)", area, true);
      if (!strings) {
        wrap.appendChild(
          element("p", { class: "help" }, "Not all of them are text: edit them in the JSON view.")
        );
      }
      return wrap;
    }

    function transformations(name, index, id) {
      var section = details("steps", name, "Transformations");
      var columns = schema.transformations(false);
      var steps = columns && object(columns.get(name));
      var present = steps ? steps.keys() : [];
      present.forEach(function (step) {
        var config = object(steps.get(step));
        if (!config) return;
        section.appendChild(
          checkbox(
            id + "step-" + step,
            step + " (on)",
            config.get("enabled") === true,
            "step-" + step,
            function (on) {
              config.set("enabled", on, null);
            }
          )
        );
      });
      var offered = stepNames(options.vocabulary()).filter(function (step) {
        return present.indexOf(step) < 0;
      });
      if (offered.length) {
        var row = element("div", { class: "inline-form" });
        var choices = offered.map(function (step) {
          return [step, step];
        });
        var choice = select(choices, offered[0], "step-choice");
        var button = element("button", {
          type: "button",
          class: "secondary small",
          "data-control": "add-step",
        });
        button.textContent = "Add the step";
        button.addEventListener("click", function () {
          var step = choice.value;
          open["steps:" + name] = true;
          change(
            function (s) {
              childObject(s.transformations(true), name).set(
                step,
                new J.JSONObject([["enabled", true]])
              );
            },
            index,
            "step-" + step,
            step + " was added to " + name + "; its options are in the JSON view."
          );
        });
        row.appendChild(field(id + "step-choice", "Add a step", choice));
        row.appendChild(button);
        section.appendChild(row);
      }
      section.appendChild(
        element(
          "p",
          { class: "help" },
          "Steps run in the order listed; their options are in the JSON view."
        )
      );
      return section;
    }

    function nameInput(name, index) {
      return textInput(
        name,
        "name",
        function (value) {
          if (value === name) return null;
          if (!value) return "A column needs a name.";
          if (schema.columns().has(value)) return "There already is a column “" + value + "”.";
          open["rules:" + value] = open["rules:" + name];
          open["steps:" + value] = open["steps:" + name];
          change(
            function (s) {
              s.rename(name, value);
            },
            index,
            "name",
            "Renamed " + name + " to " + value + "."
          );
          return null;
        },
        { spellcheck: "false" }
      );
    }

    function typeFields(grid, name, index, id, definition, type) {
      var choices = type ? (type.base ? [] : [["", "Choose a type"]]).concat(TYPES) : [];
      var typeSelect = select(
        choices,
        type ? type.base : "As in the JSON (a union of types)",
        "type"
      );
      typeSelect.disabled = !type;
      typeSelect.addEventListener("change", function () {
        var base = typeSelect.value;
        change(
          function () {
            definition.set("type", type.nullable ? [base, "null"] : base, null);
          },
          index,
          "type",
          name + " is " + base + " now."
        );
      });
      grid.appendChild(field(id + "type", "Type", typeSelect));
      if (!type || type.base !== "string") return;
      var vocabulary = options.vocabulary();
      [
        ["format", "Format", afterLast(definition, ["type"])],
        [
          "x-special-type",
          "Special type (validated and formatted)",
          afterLast(definition, ["type", "format"]),
        ],
      ].forEach(function (spec) {
        var choices = [["", "None"]].concat(columnKeyValues(vocabulary, spec[0]));
        var node = select(choices, text(definition, spec[0]), spec[0]);
        node.addEventListener("change", function () {
          writer(definition, spec[0], spec[2])(node.value);
        });
        grid.appendChild(field(id + spec[0], spec[1], node));
      });
    }

    function checks(name, id, definition, type) {
      var wrap = element("div", { class: "checks" });
      var required = schema.doc.get("required");
      var boxes = [
        [
          "required",
          "Required: the file must have it",
          Array.isArray(required) && required.indexOf(name) >= 0,
          function (on) {
            setMember(schema.doc, "required", name, on, afterLast(schema.doc, ["properties"]));
          },
        ],
        [
          "nullable",
          "Nullable: values may be empty",
          type && type.nullable,
          function (on) {
            type.nullable = on;
            definition.set("type", on ? [type.base, "null"] : type.base, null);
          },
        ],
        [
          "primary",
          "Primary key: unique and never empty",
          schema.primary().indexOf(name) >= 0,
          function (on) {
            schema.setPrimary(name, on);
          },
        ],
        [
          "unique",
          "Unique: no two rows have the same value",
          schema.isUnique(name),
          function (on) {
            schema.setUnique(name, on);
          },
        ],
      ];
      boxes.forEach(function (box) {
        if (box[0] === "nullable" && !(type && type.base)) return;
        wrap.appendChild(checkbox(id + box[0], box[1], !!box[2], box[0], box[3]));
      });
      return wrap;
    }

    function moveButtons(wrap, name, index, count) {
      [
        ["up", "Move up", index - 1],
        ["down", "Move down", index + 1],
      ].forEach(function (spec) {
        var target = spec[2];
        var button = element("button", {
          type: "button",
          class: "secondary small",
          "data-control": spec[0],
        });
        button.textContent = spec[1];
        button.appendChild(element("span", { class: "visually-hidden" }, " " + name));
        button.disabled = target < 0 || target >= count;
        button.addEventListener("click", function () {
          // At the top or the bottom the same button is disabled: focus the other one
          var edge = target === 0 || target === count - 1;
          var control = edge ? (spec[0] === "up" ? "down" : "up") : spec[0];
          var where = "it is column " + (target + 1) + " of " + count + " now.";
          change(
            function (s) {
              s.move(index, target);
            },
            target,
            control,
            "Moved " + name + " " + spec[0] + ": " + where
          );
        });
        wrap.appendChild(button);
      });
    }

    function column(name, index, count) {
      var definition = object(schema.columns().get(name));
      var id = "sf-" + index + "-";
      var item = element("li", { class: "column", "data-column": String(index) });
      var set = element("fieldset");
      set.appendChild(element("legend", {}, "Column " + (index + 1) + ": " + name));
      item.appendChild(set);
      if (!definition) {
        set.appendChild(
          element(
            "p",
            { class: "muted" },
            "This column is not an object; edit it in the JSON view."
          )
        );
        return item;
      }
      var type = typeOf(definition);
      var grid = element("div", { class: "row2" });
      grid.appendChild(field(id + "name", "Name", nameInput(name, index)));
      typeFields(grid, name, index, id, definition, type);
      set.appendChild(grid);
      var description = textInput(
        text(definition, "description"),
        "description",
        writer(definition, "description")
      );
      set.appendChild(field(id + "description", "Description", description, true));
      set.appendChild(checks(name, id, definition, type));
      var shared = schema.sharedUnique(name);
      if (shared.length) {
        var together = "Also unique together with other columns: " + shared.join("; ") + ".";
        set.appendChild(
          element("p", { class: "help" }, together + " Those are edited in the JSON view.")
        );
      }
      if (type) set.appendChild(rules(name, id, definition, type));
      set.appendChild(transformations(name, index, id));

      var buttons = element("div", { class: "actions" });
      moveButtons(buttons, name, index, count);
      var remove = element("button", {
        type: "button",
        class: "danger small",
        "data-control": "remove",
      });
      remove.textContent = "Remove";
      remove.appendChild(element("span", { class: "visually-hidden" }, " " + name));
      remove.addEventListener("click", function () {
        // Focus moves to the column that takes its place (or the one before, or Add)
        var next = index < count - 1 ? index : index - 1;
        change(
          function (s) {
            s.removeColumn(name);
          },
          next,
          "name",
          "Removed " + name + "."
        );
      });
      buttons.appendChild(remove);
      set.appendChild(buttons);
      return item;
    }

    // ---------------------------------------------------------------- the whole form

    function draw() {
      list.textContent = "";
      var names = schema.columns() ? schema.columns().keys() : [];
      note.textContent = names.length ? "" : "No columns yet.";
      names.forEach(function (name, index) {
        list.appendChild(column(name, index, names.length));
      });
      list.querySelectorAll("details[data-open]").forEach(function (node) {
        node.addEventListener("toggle", function () {
          open[node.dataset.open] = node.open;
        });
      });
    }

    function unusable(message) {
      schema = null;
      printed = null;
      list.textContent = "";
      note.textContent = message;
      add.disabled = true;
    }

    add.addEventListener("click", function () {
      var count = schema.columns() ? schema.columns().entries.length : 0;
      change(
        function (s) {
          s.addColumn();
        },
        count,
        "name",
        "Added a column at the end; it is column " + (count + 1) + "."
      );
      if (document.activeElement.select) document.activeElement.select();
    });

    /* Shows ``source`` (the editor's JSON) unless the form wrote it itself; ``where(offset)``
       names a place in it. */
    function render(source, where) {
      if (source === printed && schema) return;
      var doc;
      try {
        doc = J.parse(source);
      } catch (error) {
        if (!(error instanceof J.ParseError)) throw error;
        var place = "The JSON has an error at " + where(error.from) + ": ";
        unusable(
          error.message.replace(/^Not valid JSON: /, place) +
            " Fix it in the JSON view; the form shows the columns of valid JSON."
        );
        return;
      }
      if (!object(doc)) {
        unusable(
          "A schema is a JSON object ({…}), and this document is not; edit it in the JSON view."
        );
      } else if (doc.has("properties") && !object(doc.get("properties"))) {
        unusable(
          "“properties” is not an object, so there are no columns to show; edit it in the JSON view."
        );
      } else {
        schema = new Schema(doc);
        printed = source;
        add.disabled = false;
        draw();
      }
    }

    return { render: render };
  }

  window.ForkliftSchemaForm = { create: create };
})();
