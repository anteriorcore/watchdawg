// Copyright © 2026 Anterior <tech@anterior.com>
// SPDX-License-Identifier: AGPL-3.0-only

// function signature overload to give ts info to type narrow
export function filter<T, S extends T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => msg is S, // only for non async checks, whatever
): AsyncGenerator<S>;
export function filter<T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => boolean | Promise<boolean>,
): AsyncGenerator<T>;
export async function* filter<T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => boolean | Promise<boolean>,
): AsyncGenerator<T> {
  for await (const msg of source) {
    if (await predicate(msg)) {
      yield msg;
    }
  }
}

export function takeWhile<T, S extends T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => msg is S,
): AsyncGenerator<S>;
export function takeWhile<T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => boolean | Promise<boolean>,
): AsyncGenerator<T>;
/**
 * stop-exclusive
 * e.g. stop on "y" for "x", "y", "z" will yield "x"
 */
export async function* takeWhile<T>(
  source: AsyncIterable<T>,
  predicate: (msg: T) => boolean | Promise<boolean>,
): AsyncGenerator<T> {
  for await (const msg of source) {
    if (!(await predicate(msg))) {
      break;
    }
    yield msg;
  }
}
