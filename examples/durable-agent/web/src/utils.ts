// localStorage keys of the saved chat and the run it is streaming.
export const MESSAGES_KEY = 'durable-agent:messages';
export const RUN_KEY = 'durable-agent:run';

// load reads a JSON value from localStorage, or returns fallback when it is
// missing or storage is unavailable.
export function load<T>(key: string, fallback: T): T {
  try {
    const value = localStorage.getItem(key);
    return value == null ? fallback : (JSON.parse(value) as T);
  } catch {
    return fallback;
  }
}

// save writes a JSON value to localStorage, and removes the key for null.
// Storage is a convenience: the chat works without it.
export function save(key: string, value: unknown) {
  try {
    if (value == null) localStorage.removeItem(key);
    else localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore unavailable or full storage.
  }
}

// clockTime returns the HH:MM of a "YYYY-MM-DD HH:MM" time.
export function clockTime(dateTime: string) {
  return dateTime.split(' ')[1] ?? dateTime;
}
