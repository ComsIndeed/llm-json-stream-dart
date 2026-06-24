## 1.0.0
### Added
- **Root Convenience Getters**: Added `.asMap`, `.stream`, `.future`, and `.asList()` directly to `JsonStreamParser` for much simpler root-level interactions.
  ```dart
  final parser = JsonStreamParser(stream);
  // Easy root object operations:
  parser.stream.listen((mapSnapshot) => print(mapSnapshot));
  final completeMap = await parser.future;
  ```

### Fixed
- **Native Idempotency (Flicker Fix)**: Wrapped `StringPropertyStream`, `MapPropertyStream`, and `ListPropertyStream` replayable streams in `ReplayableBroadcastStream` caching wrappers. This provides perfect reference identity stability for Flutter's `StreamBuilder` and animations during widget rebuilds, while perfectly preserving multiple subscriptions and late-subscriber buffer replay.

## 0.4.7
### Refactored
- **Removed unused variables in JsonStreamParser**


## 0.4.6
### Documentation
- **Added thinking tag skipper functionality to JsonStreamParser**
  - New `skipThoughts` parameter to ignore `<thinking>...</thinking>` tags in the input stream
  - Set custom thinking tags with `thinkingTags` parameter
  ```dart
  final parser = JsonStreamParser(
    stream, 
    skipThoughts: true, // Enable thinking tag skipping
    thinkingTags: ('<reasoning>', '</reasoning>') // Custom tags
  );
  ```

## 0.4.5
### Documentation
- **Renamed repository URLs in README and docs to use `llm-json-stream-dart`**

## 0.4.4
### Documentation
- **Minor edits to README and API docs**

## 0.4.3
### Documentation
- **Fixed broken docs**: Removed dartdoc categories that caused broken topic links on pub.dev

## 0.4.2
### Documentation
- **Enhanced API documentation**: Improved documentation comments for all public classes
- **Simplified docs structure**: Removed experimental dartdoc categories for cleaner pub.dev integration

## 0.4.1
### Performance
- **StringBuffer optimization**: Replaced String concatenation with StringBuffer in delegates and controllers for better memory efficiency
- **Type check optimization**: Replaced `runtimeType.toString()` comparisons with `is` type checks for faster delegate detection
- **Set for O(1) lookups**: Changed `_valueFirstCharacters` from List to Set in ListPropertyDelegate

### Documentation
- **Contributor documentation**: Added comprehensive internal architecture docs with Mermaid diagrams
  - Architecture overview and system design
  - Detailed delegate documentation with state machines
  - Property stream and controller architecture
  - Data flow examples with sequence diagrams
  - Path system, streaming mechanism, and nesting handler explanations
- **README links**: Added contributor documentation links to the README

## 0.4.0
### Added
- **Buffered streams for Maps and Lists**: Both `MapPropertyStream` and `ListPropertyStream` now support buffered (replayable) streams, matching `StringPropertyStream` behavior
  - `.stream` (default, recommended): Replays the latest value to new subscribers (BehaviorSubject-style), preventing race conditions when subscribing late
  - `.unbufferedStream`: Direct access to the live stream without replay, for cases where you need live-only behavior
  - **Memory efficient**: Only stores the latest state (O(1) memory), not the full history of emissions

- **`onProperty` callback for Maps**: Similar to `onElement` for lists, maps now support an `onProperty` callback that fires when each property starts parsing
  ```dart
  parser.getMapProperty('user').onProperty((property, key) {
    print('Property "$key" started parsing');
    // Subscribe to property value as it streams
  });
  ```

- **Yap Filter (`closeOnRootComplete`)**: New parser option to stop parsing after the root JSON object/array completes, ignoring any trailing text from the LLM
  ```dart
  final parser = JsonStreamParser(stream, closeOnRootComplete: true);
  // Stops after root JSON, ignores: "Hope this helps!"
  ```

- **Observability with `ParseEvent`**: Monitor parsing events in real-time with the `onLog` callback
  ```dart
  final parser = JsonStreamParser(stream, onLog: (event) {
    print('[${event.type}] ${event.message}');
  });
  ```
  - Event types: `rootStart`, `mapKeyDiscovered`, `listElementStart`, `propertyStart`, `propertyComplete`, `stringChunk`, `yapFiltered`
  - Property-specific logging via `.onLog()` method on any `PropertyStream`

### Changed
- `MapPropertyStream.stream` now returns a replayable stream that emits the latest state to new subscribers
- `ListPropertyStream.stream` now returns a replayable stream that emits the latest state to new subscribers
- **Breaking**: Buffered streams now emit only the latest value (BehaviorSubject-style) instead of full history replay. This prevents O(N²) memory usage on large streams.

### Fixed
- Fixed timing issue where `dispose()` was called before async completers finished, causing "Parser was disposed before property completed" errors
- Fixed memory leak where Map and List buffers stored every intermediate state (now stores only latest)

### Migration
If you were relying on the previous behavior where `.stream` didn't replay values:
```dart
// Before (0.3.x): .stream was unbuffered
mapStream.stream.listen(...);

// After (0.4.0): Use .unbufferedStream for the same behavior
mapStream.unbufferedStream.listen(...);

// Or use .stream (recommended) for buffered/replayable behavior
mapStream.stream.listen(...);  // Will receive latest state immediately
```

## 0.3.1
### Documentation
- Updated README import statements to use `llm_json_stream` package name

## 0.3.0
### Fixes:
- Fixed return types for getter methods
- Fixed exports
- Moved all files into `src/` folder for better package structure

## 0.2.3
### Changes
- Removed `JsonStreamParserController` as an export
- Add type casting getters for `PropertyStream` objects:
  - `asString()`
  - `asNum()`
  - `asBool()`
  - `asMap()`
  - `asList()`
### Documentation
- Updated README

## 0.2.2
### Added
- Added shorthands
- Added streams for lists and maps
### Documentation
- Updated README with new demos

## 0.2.1
### Documentation
- Updated README with new demos showcasing functionality

## 0.2.0

### Fixed
- Fixed `getMapProperty()` returning empty maps instead of populated content
- Fixed nested lists and maps within parent maps returning null values
- Fixed map property delegates not creating controllers for nested structures before child delegates
- Fixed array element maps (e.g., `items[0]`) not containing their full content

### Changed
- Map property delegates now collect all child values (primitives, maps, lists) before completing
- Improved property controller initialization order for complex nested structures

### Tests
- Added 166 comprehensive tests for map and list value retrieval across different nesting levels
- Tests cover various chunk sizes (1-50), timing intervals (0-200ms), and nesting depths (1-5 levels)

## 0.1.4
- Updated demo to use Github raw content URL

## 0.1.3
- Fixed demo not showing in Pub.dev

## 0.1.2
- Changelog fixes
- Added main example

## 0.1.1
- Minor documentation updates

## 0.1.0

### Added
- Initial release of streaming JSON parser optimized for LLM responses
- Path-based property subscriptions with chainable API
- Support for all JSON types: String, Number, Boolean, Null, Map, List
- Array index access and dynamic element callbacks
- Handles leading whitespace before root JSON elements

### Features
- Reactive property access: Subscribe to JSON properties as they complete in the stream
- Nested structures: Full support for deeply nested objects and arrays
- Chainable API: Access nested properties with fluent syntax
- Type safety: Typed property streams for all JSON types
- Memory safe: Proper stream lifecycle management and closed stream guards

### Fixed
- Root maps completing properly
- Nested maps completing correctly
- List chainable property access working
- "Cannot add event after closing" errors
- Proper delimiter handling between primitives and containers
- Child delegate completion detection
