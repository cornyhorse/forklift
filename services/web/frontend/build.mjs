// Builds the schema editor's two generated files (run `npm run build`; see README.md):
//
//   static/ui/vendor/codemirror.min.js       CodeMirror 6 as one script (window.CodeMirror),
//                                            plus codemirror.LICENSE.txt with its notices
//   static/ui/schema-vocabulary.json         what completion offers, from schema-standards/*.json
//
// Both are committed; CI rebuilds them and fails if the result differs.

import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const here = dirname(fileURLToPath(import.meta.url));
const staticDir = join(here, "..", "src", "forklift_web", "ui", "static", "ui");
const standardsDir = join(here, "..", "..", "..", "schema-standards");

// ------------------------------------------------------------------------------------- bundle

function packageOf(input) {
  const match = /node_modules\/((?:@[^/]+\/)?[^/]+)\//.exec(input);
  return match && match[1];
}

async function bundle() {
  const result = await build({
    entryPoints: [join(here, "src", "codemirror.js")],
    bundle: true,
    minify: true,
    format: "iife",
    globalName: "CodeMirror",
    target: ["es2020"],
    legalComments: "none",
    metafile: true,
    write: false,
    logLevel: "warning",
  });
  const names = [...new Set(Object.keys(result.metafile.inputs).map(packageOf))]
    .filter(Boolean)
    .sort();
  const packages = names.map((name) => {
    const root = join(here, "node_modules", name);
    const meta = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
    const file = readdirSync(root).find((entry) => /^licen[cs]e/i.test(entry));
    return {
      name,
      version: meta.version,
      license: meta.license,
      text: readFileSync(join(root, file), "utf8"),
    };
  });
  const banner =
    "/*! CodeMirror 6 for the forklift schema editor, built by services/web/frontend. " +
    packages.map((p) => `${p.name} ${p.version} (${p.license})`).join(", ") +
    ". Licenses: codemirror.LICENSE.txt */\n";
  writeFileSync(
    join(staticDir, "vendor", "codemirror.min.js"),
    banner + result.outputFiles[0].text
  );
  const notices = packages.map((p) => `${p.name} ${p.version}\n\n${p.text.trim()}\n`);
  writeFileSync(
    join(staticDir, "vendor", "codemirror.LICENSE.txt"),
    "Third-party software in codemirror.min.js\n" +
      "=========================================\n\n" +
      notices.join("\n-------------------------------------------------------------------\n\n")
  );
}

// ------------------------------------------------------------------------------------- vocabulary

// What the keywords of JSON Schema itself mean in a forklift schema (the standards show them
// without explanation). Extension keys take their text from the "description" the standards
// give them.
const KEYWORDS = {
  $schema: "The JSON Schema dialect; forklift reads 2020-12.",
  $id: "The schema's identifier, under https://github.com/cornyhorse/forklift/schema-standards/.",
  title: "A short name for the schema (required by the engine).",
  description: "What this is, for people reading the schema.",
  type: 'The JSON type: string, integer, number, boolean, array or object; add "null" (as a list) for a nullable column.',
  properties:
    "The columns, in file order: a file without a header row gets its column names from this order.",
  required: "The columns that must be in the file.",
  additionalProperties: "Whether columns the schema does not name are allowed.",
  format: "A string's format: date, date-time, email or uuid.",
  enum: "The only values the column may hold.",
  minimum: "The smallest value allowed.",
  maximum: "The largest value allowed.",
  minLength: "The shortest text allowed.",
  maxLength: "The longest text allowed.",
  pattern: "A regular expression every value must match.",
  multipleOf: "Values must be a multiple of this number.",
  items: "The type of a list's items.",
  contentEncoding: "How binary values are written as text, such as base64.",
  "x-special-type":
    "Validates and formats a well-known kind of value (ssn, zip-5, phone, email, ipv4, ...).",
};
// Keys whose string values are free text or examples rather than choices.
const FREE_TEXT = new Set([
  "$id",
  "title",
  "description",
  "description_detail",
  "pattern",
  "expression",
  "name",
  "value",
  "keywords",
  "columnName",
  "outputName",
]);

function node() {
  return { kinds: [] };
}

function kindOf(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
}

function addUnique(list, value) {
  if (!list.includes(value)) list.push(value);
}

// The top-level keys say which standards define them ("in": "csv excel").
const root = node();

// The document and each column's definition: their keys are JSON Schema's keywords.
function isSchemaLevel(target) {
  return target === root || target === (root.keys?.properties?.columnKeys ?? null);
}

function child(parent, part, key, standard) {
  parent[part] = parent[part] || {};
  const found = parent[part][key] || (parent[part][key] = node());
  if (parent === root) {
    const standards = found.in ? found.in.split(" ") : [];
    addUnique(standards, standard);
    found.in = standards.join(" ");
  }
  if (isSchemaLevel(parent) && KEYWORDS[key] && !found.doc) found.doc = KEYWORDS[key];
  return found;
}

function suggestible(key, value) {
  return !FREE_TEXT.has(key) && value.length <= 64 && !/\s/.test(value);
}

// Merges one example value into the vocabulary node that describes its place in a document.
function learn(target, value, key, context) {
  addUnique(target.kinds, kindOf(value));
  if (typeof value === "string" && suggestible(key, value)) {
    target.values = target.values || [];
    addUnique(target.values, value);
  }
  if (Array.isArray(value)) {
    target.items = target.items || node();
    if (value.length && value.every((item) => context.columns.has(item))) {
      // A list of column names: completion offers the document's own columns here
      target.columnNames = true;
      addUnique(target.items.kinds, "string");
      return;
    }
    for (const item of value) learn(target.items, item, key, context);
    return;
  }
  if (value === null || typeof value !== "object") return;
  const entries = Object.entries(value);
  const byColumn =
    key === "properties" || (entries.length && entries.every(([k]) => context.columns.has(k)));
  if (byColumn) {
    target.columnKeys = target.columnKeys || node();
    for (const [, item] of entries) learn(target.columnKeys, item, "", context);
    return;
  }
  for (const [name, item] of entries) {
    if (
      name === "description" &&
      typeof item === "string" &&
      !target.doc &&
      !isSchemaLevel(target)
    ) {
      target.doc = item; // an extension's description says what it is for
    }
    learn(child(target, "keys", name, context.standard), item, name, context);
  }
}

// The SQL standard describes x-sql with JSON Schema (and gives example documents).
function learnSchema(target, schema, standard) {
  if (schema.description && !target.doc) target.doc = schema.description;
  for (const kind of [].concat(schema.type || [])) addUnique(target.kinds, kind);
  if (Array.isArray(schema.enum)) target.values = schema.enum.filter((v) => typeof v === "string");
  for (const [name, item] of Object.entries(schema.properties || {})) {
    learnSchema(child(target, "keys", name, standard), item, standard);
  }
  if (schema.items && typeof schema.items === "object") {
    target.items = target.items || node();
    learnSchema(target.items, schema.items, standard);
  }
}

function isSchema(value) {
  return value && typeof value === "object" && "type" in value && "properties" in value;
}

function vocabulary() {
  const files = readdirSync(standardsDir)
    .filter((name) => name.endsWith(".json"))
    .sort();
  for (const file of files) {
    const standard = file.replace(/^\d+-/, "").replace(/\.json$/, "");
    const document = JSON.parse(readFileSync(join(standardsDir, file), "utf8"));
    const columns = new Set(Object.keys(document.properties || {}));
    const meta = [...columns].some((name) => name.startsWith("x-"));
    if (!meta) {
      learn(root, document, "", { standard, columns });
      continue;
    }
    // A standard whose "properties" are root keys: those are JSON Schema (or example values),
    // and its "examples" are documents.
    const rest = { ...document, properties: undefined, required: undefined, examples: undefined };
    learn(root, JSON.parse(JSON.stringify(rest)), "", { standard, columns: new Set() });
    for (const [name, value] of Object.entries(document.properties)) {
      const target = child(root, "keys", name, standard);
      if (isSchema(value)) learnSchema(target, value, standard);
      else learn(target, value, name, { standard, columns: new Set() });
    }
    for (const example of document.examples || []) {
      learn(root, example, "", { standard, columns: new Set() });
    }
  }
  return { standards: files, root };
}

await bundle();
writeFileSync(join(staticDir, "schema-vocabulary.json"), JSON.stringify(vocabulary()) + "\n");
