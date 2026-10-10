// The parts of CodeMirror 6 the schema editor uses, bundled into one script that sets
// window.CodeMirror (static/ui/vendor/codemirror.min.js). The editor itself is plain
// JavaScript in static/ui/schema-editor.js, which needs no build step.

export { Prec } from "@codemirror/state";
export {
  EditorView,
  drawSelection,
  highlightActiveLine,
  highlightActiveLineGutter,
  highlightSpecialChars,
  keymap,
  lineNumbers,
} from "@codemirror/view";
export { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
export {
  HighlightStyle,
  bracketMatching,
  indentOnInput,
  syntaxHighlighting,
} from "@codemirror/language";
export { tags } from "@lezer/highlight";
export { json } from "@codemirror/lang-json";
export { linter, lintGutter, lintKeymap } from "@codemirror/lint";
export {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
} from "@codemirror/autocomplete";
