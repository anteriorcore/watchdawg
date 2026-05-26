// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

import { deepStrictEqual, ok } from "node:assert/strict";
import test, { describe } from "node:test";
import { filter, takeWhile } from "./pipeline.ts";

// helper since node has array from generator but not vv
async function* arrayToGenerator<T>(arr: T[]): AsyncGenerator<T> {
  yield* arr;
}
void describe("demo pipeline ops", async () => {
  void test("test helper function", async () => {
    const arr = [1, 2, 3];
    deepStrictEqual(await Array.fromAsync(arrayToGenerator(arr)), arr);
  });

  void test("take while is exclusive", async () => {
    const numbersAndNull = arrayToGenerator([1, 2, null, 3, null]);
    const everythingBeforeNull = takeWhile(numbersAndNull, (e) => e !== null);
    deepStrictEqual(await Array.fromAsync(everythingBeforeNull), [1, 2]);

    // example from the docstring
    const xyz = arrayToGenerator(["x", "y", "z"]);
    const stopAtY = takeWhile(xyz, (e) => e !== "y");
    deepStrictEqual(await Array.fromAsync(stopAtY), ["x"]);
  });

  void test("the filter functions work for type narrowing", async () => {
    const src = filter(
      arrayToGenerator([null, 1, null, 2, 3]),
      (e) => typeof e === "number",
    );
    for await (const msg of src) {
      // type system respects the narrowing
      const x: number = msg;
      ok(x + 1);
    }

    // and filter works
    const numbers = filter(
      arrayToGenerator([null, 1, null, 2, 3]),
      (e) => typeof e === "number",
    );
    deepStrictEqual(await Array.fromAsync(numbers), [1, 2, 3]);
  });
});
