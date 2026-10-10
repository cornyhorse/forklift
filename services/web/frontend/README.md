# forklift-web frontend build inputs

The UI needs no build step except for two generated files of the schema editor, which are
committed so that the gateway's package and image need no Node.js:

| Output (under `src/forklift_web/ui/static/ui/`) | Made from | What it is |
|---|---|---|
| `vendor/codemirror.min.js` | `src/codemirror.js`, `package-lock.json` | CodeMirror 6 as one minified script that sets `window.CodeMirror` (only the parts the editor uses) |
| `vendor/codemirror.LICENSE.txt` | the bundled packages | Their license notices (all MIT) |
| `schema-vocabulary.json` | `../../../schema-standards/*.json` | What completion offers at each place in a schema: keys, values, the x-* extensions and their descriptions |

The editor itself (`schema-editor.js`, `schema-form.js`, `schema-json.js`) is plain JavaScript
next to them and is not built.

## Rebuilding

With Node.js 20 or newer:

```sh
cd services/web/frontend
npm ci          # exactly the versions in package-lock.json
npm run build   # writes the three files above
```

Rebuild after changing `src/codemirror.js`, a version in `package.json` (then `npm install` to
update the lock file), `build.mjs`, or a schema standard. Commit the outputs with the change.
The build is deterministic: CI runs `npm ci && npm run build` and fails if the committed files
differ from what it builds.

## Choices

- **Versions are exact** in `package.json` (and `overrides` pins `@lezer/lr`), and new releases
  are taken deliberately, after they have been out for a while.
- **Small surface.** `src/codemirror.js` exports only what the editor uses; there is no search
  panel or folding. The bundle is about 390 KB minified (about 127 KB gzipped); most of it is
  `@codemirror/view`.
- **Content-Security-Policy.** The pages allow scripts and styles from the gateway's own files
  only. CodeMirror adds its styles at run time; in a document that would be a `<style>`
  element, which the policy refuses, so the editor is mounted in a shadow root, where CodeMirror
  uses constructed style sheets instead (the policy does not govern those). No nonce and no
  `'unsafe-inline'` are needed.
- **Vocabulary.** `build.mjs` reads each standard as an example document (the SQL standard's
  `x-sql` as the JSON Schema it is), merges what appears at each place, recognises objects keyed
  by column names and lists of column names (where completion offers the document's own
  columns), and keeps short string values as suggestions. JSON Schema's own keywords get a
  one-line explanation from `build.mjs`; the extensions use the `description` the standards
  give them.
