# The schema editor

Authors write schemas in the UI's editor (Schemas, New schema; or a schema's "Edit (save as
version n)"). Saving never changes an earlier version: it creates a new one, with exactly the
JSON the editor shows. Editing needs the `schemas:write` scope (Authors and Admins); starting
from a sample and checking against a file run jobs, which also need `jobs:run`.

The page works without JavaScript: the document is then a plain text area, and saving, the
gateway's checks on save and "Check the draft" (a normal form post) all work.

## The JSON view

A CodeMirror editor with JSON highlighting, bracket matching and line numbers.

- **Syntax errors show at once**, underlined where they are, and in the status line under the
  editor ("Not valid JSON yet (line 4, column 12): expected , or } after the value of "id"."). A
  key that appears twice in an object is a warning: only the last one would be kept.
- **The gateway's review** runs after a short pause in typing, once the text is valid JSON. It
  is the check saving makes (a JSON object, within the `schema_max_bytes` setting), plus the
  mistakes in shape the engine refuses when it loads a schema: a missing or wrong `$schema`,
  `$id` (under `https://github.com/cornyhorse/forklift/schema-standards/`), `title` or
  `"type": "object"`; `properties` that is not an object; a column that is not an object, has
  no type or an unknown one; `minimum`/`maximum` and `minLength`/`maxLength` that are not
  numbers or are the wrong way round; and `required`, `x-primaryKey.columns` and
  `x-uniqueConstraints` naming columns the schema does not have (not checked when the schema
  has an `x-columnMapping`, whose output names the gateway does not work out). Each problem is
  marked at its place in the JSON and listed under the editor; the line number in the list
  moves the cursor there. Only the first kind stops saving; the others are what a job would
  fail with. The gateway never runs a schema's regular expressions or expressions: checking a
  draft against data is "Check it against a file", on a worker.
- **Completion** (Ctrl+Space, or as you type a key or a value) offers what the schema
  standards in `schema-standards/` use at that place: the top-level keys and extensions (with
  the formats that define them and what they are for), a column's keywords (`type`, `format`,
  `x-special-type`, `minimum`, ...), their values (`integer`, `date-time`, `zip-5`, ...), the
  keys and values inside each extension, and the schema's own column names where a column
  belongs (in `required`, `x-primaryKey.columns`, `x-transformations.column_transformations`,
  `x-csv.nulls.perColumn`, ...).
- **Format the JSON** re-indents the document, keeping key order and numbers as written.

### Keyboard

| Keys | Do |
|---|---|
| Tab, Shift+Tab | Move to the next or previous control, as anywhere on the page: the editor does not keep the focus |
| Enter | A new line, indented |
| Ctrl+] and Ctrl+[ | Indent and outdent the lines |
| Ctrl+Space | List what fits at the cursor; arrows choose, Enter takes it, Escape closes the list |
| F8, Ctrl+Shift+M | Next problem; the list of problems |
| Ctrl+Z, Ctrl+Shift+Z | Undo and redo, including changes made in the Columns view |
| Ctrl+Enter | Check the draft against the file chosen in "Check it against a file" |

(Cmd instead of Ctrl on a Mac.) The status line is announced to screen readers when it changes.

## The Columns view

"Columns" shows the columns of `properties` as a form, in their order, which matters: a file
without a header row gets its column names from it. Each change goes into the JSON at once, and
everything the form does not show stays exactly as it was (other keys of a column, other
extensions, the order of keys, numbers as written).

For each column:

| Field | In the JSON |
|---|---|
| Name | The key in `properties`. Renaming also renames it in `required`, `x-primaryKey.columns`, `x-uniqueConstraints` and `x-transformations.column_transformations` |
| Type, Nullable | `"type": "string"`, or `["string", "null"]` when nullable. A union (`anyOf`, several types) is shown but edited in the JSON |
| Format, Special type | `format` and `x-special-type` (text columns), with the values the standards use |
| Description | `description` |
| Required | The column is in `required` |
| Primary key | The column is in `x-primaryKey.columns` (the key is removed with its last column; a `type` of single/composite is kept in step) |
| Unique | A one-column entry `{"name": "<column>_unique", "columns": ["<column>"]}` in `x-uniqueConstraints`; constraints over several columns are listed and edited in the JSON |
| Rules | `minimum`/`maximum` (numbers), `minLength`/`maxLength`, `pattern` and `enum` (text) |
| Transformations | Which steps of `x-transformations.column_transformations.<column>` are on (`enabled`), and adding a step (`string_cleaning`, `money_conversion`, `datetime`); a step's options are edited in the JSON |

"Move up", "Move down", "Remove" and "Add a column" are buttons, so the order can be changed
with the keyboard; focus stays on the column moved, and each change is announced. Removing a
column also removes it from `required`, the primary key, its one-column unique constraint and
its transformations; other references (such as `x-csv.nulls.perColumn`) stay, and the review
points out those it checks.

When the JSON has a syntax error, the Columns view says where and waits for the JSON view to
fix it.

## Starting points

- **Start from a sample**: choose one of your uploads; a worker generates a schema from it (a
  `generate_schema` job), and "Use it in the editor" replaces the draft with it. Undo brings the
  earlier draft back. Without JavaScript, "Generate a schema" opens the job's page, where the
  generated `schema.json` can be downloaded. A schema generated from an upload's page opens in
  the editor too ("Edit and save it as a new schema" on the job's page).
- **Check it against a file**: a worker reads the header and a sample of rows of an upload (or
  of a dataset's source) with the draft as it is, and the result appears beside the editor;
  nothing is saved. "Check again whenever I pause typing" repeats it after each pause.

## How it is built

The editor is CodeMirror 6, bundled into `ui/static/ui/vendor/codemirror.min.js` from
`services/web/frontend/` (see its README; CI checks the committed bundle is what the lock file
builds). The pages' Content-Security-Policy allows scripts and styles from the gateway's files
only; the editor is mounted in a shadow root, where CodeMirror's styles are constructed style
sheets the policy allows, so the policy has no nonce or `'unsafe-inline'`. The review is
`POST /schemas/check/` (a UI endpoint returning `{"problems": [{"path", "message",
"blocking"}]}`, paths as lists of keys and indexes); it uses `services.schemas.review_document`.
