// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

import prettier from "eslint-config-prettier";
import tseslint from "typescript-eslint";
import { defineConfig } from "eslint/config";

export default defineConfig(
  tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  prettier,
  {
    languageOptions: {
      parserOptions: {
        projectService: {
          allowDefaultProject: ["*.js"],
        },
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      /**
       * Below are the rules we want to enforce
       */
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/no-explicit-any": "warn", // be responsible!
      "@typescript-eslint/no-empty-function": "warn",
      "@typescript-eslint/require-await": "error",
      "@typescript-eslint/consistent-type-definitions": "off",
      "@typescript-eslint/no-redundant-type-constituents": "warn", // helpful to humans
      "@typescript-eslint/no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "node:assert",
              message: "Use node:assert/strict instead.",
            },
          ],
        },
      ],
      "@typescript-eslint/switch-exhaustiveness-check": "error",
    },
  },
);
