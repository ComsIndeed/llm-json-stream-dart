import 'dart:async';

import 'list_property_delegate.dart';
import 'map_property_delegate.dart';
import 'parse_event.dart';
import 'property_delegate.dart';
import 'property_stream.dart';
import 'property_stream_controller.dart';
import 'property_getter_mixin.dart';

/// A streaming JSON parser optimized for LLM responses.
///
/// This parser processes JSON data character-by-character as it streams in,
/// allowing you to react to properties before the entire JSON is received.
/// It's specifically designed for handling Large Language Model (LLM) streaming
/// responses that output structured JSON data.
///
/// ## Key Features
///
/// - **Reactive property access**: Subscribe to JSON properties as they complete
/// - **Streaming string values**: Receive string content chunk-by-chunk as it arrives
/// - **Path-based subscriptions**: Access nested properties with dot notation or chainable API
/// - **Type safety**: Typed property streams for all JSON types
/// - **Dynamic list handling**: React to array elements as soon as they start arriving
/// - **Yap filter**: Automatically stops parsing after root JSON object completes
/// - **Observability**: Optional logging callbacks for debugging and monitoring
/// - **Thinking tag skipper**: Optionally skip content inside thinking/reasoning tags
///
/// ## Basic Usage
///
/// ```dart
/// final parser = JsonStreamParser(streamFromLLM);
///
/// // Subscribe to properties
/// parser.getStringProperty('title').stream.listen((chunk) {
///   print('Title chunk: $chunk');
/// });
///
/// // Wait for complete values
/// final age = await parser.getNumberProperty('user.age').future;
/// print('Age: $age');
/// ```
///
/// ## Path Syntax
///
/// Access nested properties using dot notation:
/// - `'title'` - Root property
/// - `'user.name'` - Nested property
/// - `'items[0].name'` - Array element property
/// - `'data.users[2].profile.age'` - Deep nesting
///
/// ## Thinking Tag Skipper
///
/// Many LLMs emit "thinking" or "reasoning" content before the actual JSON
/// response, wrapped in tags like `<think>...</think>`. Set [skipThoughts]
/// to `true` to automatically skip this content:
///
/// ```dart
/// final parser = JsonStreamParser(
///   streamFromLLM,
///   skipThoughts: true,
///   // Optional: customize the tags (defaults to <think> and </think>)
///   thinkingTags: ('<reasoning>', '</reasoning>'),
/// );
/// ```
///
/// Disposal
///
/// Call [dispose] when done to clean up resources:
/// ```dart
/// await parser.dispose();
/// ```
class JsonStreamParser with PropertyGetterMixin {
  /// Creates a new JSON stream parser that processes the given [stream].
  ///
  /// The parser immediately begins consuming the stream and parsing JSON
  /// character-by-character.
  ///
  /// If [closeOnRootComplete] is true (the default), the parser will
  /// automatically stop parsing and dispose itself when the root JSON object
  /// or array is fully parsed. This prevents issues with LLMs that add
  /// extra text after the JSON (the "yap" problem).
  ///
  /// If [skipThoughts] is true, the parser will skip content inside thinking
  /// tags (e.g., `<think>...</think>`). This is useful for LLMs that emit
  /// reasoning content before the actual JSON response. Defaults to false.
  ///
  /// The [thinkingTags] parameter allows you to customize the start and end
  /// delimiters for thinking tags. Defaults to `('<think>', '</think>')`.
  /// You can set it to any pair of strings, e.g., `('[thought]', '[/thought]')`.
  ///
  /// If [onLog] is provided, it will be called with [ParseEvent] objects
  /// for various parsing events, useful for debugging and monitoring.
  ///
  /// Example:
  /// ```dart
  /// final parser = JsonStreamParser(myLLMResponseStream);
  /// ```
  ///
  /// Example with thinking tag skipping:
  /// ```dart
  /// final parser = JsonStreamParser(
  ///   myLLMResponseStream,
  ///   skipThoughts: true,
  ///   thinkingTags: ('<reasoning>', '</reasoning>'),
  /// );
  /// ```
  JsonStreamParser(
    Stream<String> stream, {
    this.closeOnRootComplete = true,
    this.skipThoughts = false,
    this.thinkingTags = const ('<think>', '</think>'),
    void Function(ParseEvent)? onLog,
  })  : _stream = stream,
        _onLog = onLog {
    _streamSubscription = _stream.listen(
      (chunk) {
        try {
          _parseChunk(chunk);
        } catch (e) {
          // This outer catch should prevent exceptions from escaping to the zone
          // The inner catch in _parseChunk should handle most cases, but this is a safety net
        }
      },
      onError: (error, stackTrace) {
        _emitLog(ParseEvent(
          type: ParseEventType.error,
          propertyPath: '',
          message: 'Stream error: $error',
          data: error,
        ));
      },
    );
    _controller = JsonStreamParserController(
      addPropertyChunk: _addPropertyChunk,
      getPropertyStreamController: _getControllerForPath,
      getPropertyStream: _getPropertyStream,
      emitLog: _emitLog,
    );
  }

  /// Returns the root JSON object as a [MapPropertyStream].
  MapPropertyStream get asMap => getMapProperty('');

  /// A stream that emits the root map snapshots as they are parsed.
  Stream<Map<String, dynamic>> get stream => asMap.stream;

  /// A future that completes with the full parsed root JSON object.
  Future<Map<String, dynamic>> get future => asMap.future;

  /// Returns the root JSON array as a [ListPropertyStream].
  ListPropertyStream<E> asList<E extends Object?>() => getListProperty<E>('');


  /// Whether to automatically stop parsing when the root JSON object completes.
  ///
  /// When true (default), the parser will dispose itself after the root
  /// object or array closes, ignoring any trailing text. This is useful
  /// for handling LLM responses that may include explanatory text after
  /// the JSON.
  ///
  /// When false, the parser continues listening to the stream until
  /// explicitly disposed or the stream closes.
  final bool closeOnRootComplete;

  /// Whether to skip content inside thinking/reasoning tags.
  ///
  /// When true, the parser will skip all content between the opening and
  /// closing thinking tags (defined by [thinkingTags]). This is useful for
  /// LLMs that emit chain-of-thought reasoning before the actual JSON.
  ///
  /// When false (default), thinking tags are treated as regular text, which
  /// may cause parse errors if they appear before the JSON.
  final bool skipThoughts;

  /// The start and end delimiters for thinking tags.
  ///
  /// Defaults to `('<think>', '</think>')`. The parser will skip all content
  /// between these tags when [skipThoughts] is true.
  ///
  /// Example custom tags:
  /// ```dart
  /// thinkingTags: ('[thought]', '[/thought]')
  /// thinkingTags: ('<reasoning>', '</reasoning>')
  /// ```
  final (String, String) thinkingTags;

  /// The logging callback for this parser.
  void Function(ParseEvent)? _onLog;

  /// Emits a log event to all registered listeners.
  void _emitLog(ParseEvent event) {
    _onLog?.call(event);
  }

  /// Gets a stream for a string property at the specified [propertyPath].
  ///
  /// Returns a [StringPropertyStream] that provides:
  /// - `.stream` - Emits string chunks as they are parsed
  /// - `.future` - Completes with the full string value
  ///
  /// Use the stream for properties where you want to display content as it
  /// arrives (e.g., displaying AI-generated text as it's typed).
  ///
  /// Example:
  /// ```dart
  /// final titleStream = parser.getStringProperty('title');
  ///
  /// // React to chunks
  /// titleStream.stream.listen((chunk) {
  ///   print('Chunk: $chunk');
  /// });
  ///
  /// // Or wait for complete value
  /// final fullTitle = await titleStream.future;
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not a string.
  @override
  StringPropertyStream getStringProperty(String propertyPath) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath] is! StringPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a StringPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => StringPropertyStreamController(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as StringPropertyStreamController;
    return controller.propertyStream;
  }

  /// Gets a stream for a number property at the specified [propertyPath].
  ///
  /// Returns a [NumberPropertyStream] that provides:
  /// - `.stream` - Emits the number when complete
  /// - `.future` - Completes with the parsed number value
  ///
  /// Example:
  /// ```dart
  /// final age = await parser.getNumberProperty('user.age').future;
  /// print('Age: $age');
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not a number.
  @override
  NumberPropertyStream getNumberProperty(String propertyPath) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath] is! NumberPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a NumberPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => NumberPropertyStreamController(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as NumberPropertyStreamController;
    return controller.propertyStream;
  }

  /// Gets a stream for a boolean property at the specified [propertyPath].
  ///
  /// Returns a [BooleanPropertyStream] that provides:
  /// - `.stream` - Emits the boolean when complete
  /// - `.future` - Completes with the parsed boolean value
  ///
  /// Example:
  /// ```dart
  /// final isActive = await parser.getBooleanProperty('user.active').future;
  /// print('Active: $isActive');
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not a boolean.
  @override
  BooleanPropertyStream getBooleanProperty(String propertyPath) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath]
            is! BooleanPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a BooleanPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => BooleanPropertyStreamController(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as BooleanPropertyStreamController;
    return controller.propertyStream;
  }

  /// Gets a stream for a null property at the specified [propertyPath].
  ///
  /// Returns a [NullPropertyStream] that provides:
  /// - `.stream` - Emits null when the property completes
  /// - `.future` - Completes with null
  ///
  /// Example:
  /// ```dart
  /// await parser.getNullProperty('optionalField').future;
  /// print('Field is null');
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not null.
  @override
  NullPropertyStream getNullProperty(String propertyPath) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath] is! NullPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a NullPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => NullPropertyStreamController(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as NullPropertyStreamController;
    return controller.propertyStream;
  }

  /// Gets a stream for a map (object) property at the specified [propertyPath].
  ///
  /// Returns a [MapPropertyStream] that provides:
  /// - `.future` - Completes with the full parsed map
  /// - Chainable property getters to access nested properties
  ///
  /// The returned stream is chainable, allowing you to access nested properties:
  /// ```dart
  /// final userMap = parser.getMapProperty('user');
  /// final name = userMap.getStringProperty('name');
  /// final age = userMap.getNumberProperty('age');
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not a map.
  @override
  MapPropertyStream getMapProperty(String propertyPath) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath] is! MapPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a MapPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => MapPropertyStreamController(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as MapPropertyStreamController;
    return controller.propertyStream;
  }

  /// Gets a stream for a list (array) property at the specified [propertyPath].
  ///
  /// Returns a [ListPropertyStream] that provides:
  /// - `.future` - Completes with the full parsed list
  /// - `.onElement()` - Callback that fires when each element starts parsing
  /// - Chainable property getters to access elements
  ///
  /// The optional [onElement] callback fires immediately when a new array
  /// element is discovered, before it's fully parsed. This enables "arm the trap"
  /// behavior for building reactive UIs:
  ///
  /// ```dart
  /// final items = parser.getListProperty('items', onElement: (element, index) {
  ///   print('New item at index $index started');
  ///
  ///   if (element is MapPropertyStream) {
  ///     element.getStringProperty('name').stream.listen((name) {
  ///       print('Item $index name: $name');
  ///     });
  ///   }
  /// });
  /// ```
  ///
  /// Throws [Exception] if the property at this path is not a list.
  @override
  ListPropertyStream<E> getListProperty<E extends Object?>(
    String propertyPath, {
    void Function(PropertyStream propertyStream, int index)? onElement,
  }) {
    if (_propertyControllers[propertyPath] != null &&
        _propertyControllers[propertyPath] is! ListPropertyStreamController) {
      throw Exception(
        'Property at path $propertyPath is not a ListPropertyStream',
      );
    }
    final controller = _propertyControllers.putIfAbsent(
      propertyPath,
      () => ListPropertyStreamController<E>(
        parserController: _controller,
        propertyPath: propertyPath,
      ),
    ) as ListPropertyStreamController<E>;
    if (onElement != null) {
      controller.addOnElementCallback(onElement);
    }
    return controller.propertyStream;
  }

  // * Controller methods
  void _addPropertyChunk<T>({required String propertyPath, required T chunk}) {
    final controller = _propertyControllers.putIfAbsent(propertyPath, () {
      if (T == String) {
        return StringPropertyStreamController(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else if (T == num) {
        return NumberPropertyStreamController(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else if (T == bool) {
        return BooleanPropertyStreamController(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else if (T == Null) {
        return NullPropertyStreamController(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else if (T == Map<String, Object?>) {
        return MapPropertyStreamController(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else if (T == List<Object?>) {
        return ListPropertyStreamController<Object?>(
          parserController: _controller,
          propertyPath: propertyPath,
        );
      } else {
        throw UnimplementedError(
          'No PropertyStreamController for type $T',
        );
      }
    }) as PropertyStreamController<
        T>; // TODO: Fix casting. Maybe remove generics?

    // everything but list and map controllers will emit chunks in its stream
    if (controller is MapPropertyStreamController ||
        controller is ListPropertyStreamController) {
      controller.complete(chunk);
      return;
    } else {
      if (controller is StringPropertyStreamController) {
        final stringController = controller as StringPropertyStreamController;
        stringController.addChunk(chunk as String);
      } else if (controller is NumberPropertyStreamController) {
        final numberController = controller as NumberPropertyStreamController;
        if (!numberController.isClosed) {
          numberController.streamController.add(chunk as num);
          numberController.complete(chunk as num);
        }
      } else if (controller is BooleanPropertyStreamController) {
        final booleanController = controller as BooleanPropertyStreamController;
        if (!booleanController.isClosed) {
          booleanController.streamController.add(chunk as bool);
          booleanController.complete(chunk as bool);
        }
      } else if (controller is NullPropertyStreamController) {
        final nullController = controller as NullPropertyStreamController;
        if (!nullController.isClosed) {
          nullController.streamController.add(chunk as Null);
          nullController.complete(chunk as Null);
        }
      }
    }
  }

  // * Fields
  final Stream<String> _stream;
  late final StreamSubscription<String> _streamSubscription;
  late final JsonStreamParserController _controller;
  bool _isDisposed = false;

  // * Memories
  final Map<String, PropertyStreamController> _propertyControllers = {};

  // * States
  PropertyDelegate? _rootDelegate;

  // * Thinking tag skipper state
  /// Whether we are currently inside thinking tags
  bool _insideThinkingTags = false;

  /// Buffer for detecting start/end tag patterns
  String _tagBuffer = '';

  /// Track if we've seen content that looks like potential thinking tags
  /// (when skipThoughts is false) for better error messages
  bool _sawPotentialThinkingTags = false;

  /// Buffer to detect potential opening tags when skipThoughts is disabled
  String _potentialTagBuffer = '';

  // * Helpers
  PropertyStreamController _getControllerForPath(String propertyPath) {
    return _propertyControllers[propertyPath]!;
  }

  PropertyStream _getPropertyStream(String propertyPath, Type streamType) {
    // If controller already exists (e.g., user called getXxxProperty before parsing),
    // check if the type matches what we're trying to parse
    final existingController = _propertyControllers[propertyPath];
    if (existingController != null) {
      // Verify type compatibility
      final isCompatible = (streamType == String &&
              existingController is StringPropertyStreamController) ||
          (streamType == num &&
              existingController is NumberPropertyStreamController) ||
          (streamType == bool &&
              existingController is BooleanPropertyStreamController) ||
          (streamType == Null &&
              existingController is NullPropertyStreamController) ||
          (streamType == Map &&
              existingController is MapPropertyStreamController) ||
          (streamType == List &&
              existingController is ListPropertyStreamController);

      if (isCompatible) {
        return existingController.propertyStream;
      } else {
        // Type mismatch - complete the existing controller with an error
        // The existing controller is what the USER requested
        // The streamType is what we FOUND in the JSON
        final requestedTypeName = existingController.runtimeType
            .toString()
            .replaceAll('PropertyStreamController', '');
        final foundTypeName = streamType.toString();

        // Build error message with potential suggestion about thinking tags
        var errorMessage =
            'Type mismatch at path "$propertyPath": requested $requestedTypeName but found $foundTypeName in JSON';

        // If we saw potential thinking tags, suggest enabling skipThoughts
        if (_sawPotentialThinkingTags && !skipThoughts) {
          errorMessage +=
              '\n\nHint: The input may contain thinking/reasoning tags before the JSON. '
              'Try setting skipThoughts: true in the JsonStreamParser constructor.';
        }

        final error = Exception(errorMessage);

        // Emit error log event
        _emitLog(ParseEvent(
          type: ParseEventType.error,
          propertyPath: propertyPath,
          message:
              'Type mismatch: requested $requestedTypeName but found $foundTypeName',
          data: error,
        ));

        if (!existingController.completer.isCompleted) {
          existingController.completer.completeError(error);
        }

        // Return null or throw - for now we throw so the parser stops processing this property
        // But we rethrow so the catch in _parseChunk can handle it gracefully
        throw error;
      }
    }

    // Otherwise create the appropriate controller based on type
    if (streamType == String) {
      return getStringProperty(propertyPath);
    } else if (streamType == num) {
      return getNumberProperty(propertyPath);
    } else if (streamType == bool) {
      return getBooleanProperty(propertyPath);
    } else if (streamType == Null) {
      return getNullProperty(propertyPath);
    } else if (streamType == Map) {
      return getMapProperty(propertyPath);
    } else if (streamType == List) {
      return getListProperty(propertyPath);
    } else {
      throw Exception('Unknown stream type: $streamType');
    }
  }

  void _parseChunk(String chunk) {
    // Check if we should stop parsing (yap filter)
    if (_isDisposed) return;
    if (closeOnRootComplete && _rootDelegate != null && _rootDelegate!.isDone) {
      // Root object is complete, stop parsing - yap filter triggered
      _emitLog(ParseEvent(
        type: ParseEventType.yapFiltered,
        propertyPath: '',
        message: 'Yap filter triggered - ignoring text after root JSON',
      ));
      // Just cancel the stream subscription to stop parsing more input
      // Don't call dispose() here as that would error out pending completers
      _streamSubscription.cancel();
      return;
    }

    try {
      for (final character in chunk.split('')) {
        // Check again inside the loop in case root completes mid-chunk
        if (closeOnRootComplete &&
            _rootDelegate != null &&
            _rootDelegate!.isDone) {
          _emitLog(ParseEvent(
            type: ParseEventType.yapFiltered,
            propertyPath: '',
            message: 'Yap filter triggered - ignoring text after root JSON',
          ));
          // Just cancel the stream subscription to stop parsing more input
          // Don't call dispose() here as that would error out pending completers
          _streamSubscription.cancel();
          return;
        }

        // Handle thinking tag skipping if enabled
        if (skipThoughts) {
          final processedChar = _processCharacterForThinkingTags(character);
          if (processedChar == null) {
            // Character was consumed by thinking tag logic, skip normal parsing
            continue;
          }
          // If processedChar is returned, it means we should process it normally
          // (This happens when we're not inside thinking tags)
        } else {
          // Even when skipThoughts is disabled, detect potential thinking tags
          // so we can provide helpful error messages
          _detectPotentialThinkingTags(character);
        }

        if (_rootDelegate != null) {
          _rootDelegate!.addCharacter(character);
          continue;
        }

        // Skip leading whitespace before the root element
        if (character == ' ' ||
            character == '\t' ||
            character == '\n' ||
            character == '\r') {
          continue;
        }

        if (character == '{') {
          _emitLog(ParseEvent(
            type: ParseEventType.rootStart,
            propertyPath: '',
            message: 'Started parsing root object',
          ));
          _rootDelegate = MapPropertyDelegate(
            propertyPath: '',
            parserController: _controller,
          );
          _rootDelegate!.addCharacter(character);
        }

        if (character == "[") {
          _emitLog(ParseEvent(
            type: ParseEventType.rootStart,
            propertyPath: '',
            message: 'Started parsing root array',
          ));
          _rootDelegate = ListPropertyDelegate(
            propertyPath: '',
            parserController: _controller,
          );
          _rootDelegate!.addCharacter(character);
        }

        continue;
      }

      _rootDelegate?.onChunkEnd();

      // Final check after processing the chunk
      if (closeOnRootComplete &&
          _rootDelegate != null &&
          _rootDelegate!.isDone) {
        _emitLog(ParseEvent(
          type: ParseEventType.yapFiltered,
          propertyPath: '',
          message: 'Yap filter triggered - root JSON complete',
        ));
        // Just cancel the stream subscription to stop parsing more input
        // Don't call dispose() here as that would error out pending completers
        // The completers will finish asynchronously
        _streamSubscription.cancel();
      }
    } catch (e) {
      _emitLog(ParseEvent(
        type: ParseEventType.error,
        propertyPath: '',
        message: 'Parsing error: $e',
        data: e,
      ));
      // Type mismatch or other parsing errors - already handled by completing
      // the specific controller with an error, so we just stop parsing
      return;
    }
  }

  /// Processes a character for thinking tag detection.
  ///
  /// Returns `null` if the character was consumed by thinking tag logic
  /// (either buffered for tag detection or skipped because we're inside tags).
  /// Returns the character if it should be processed normally.
  String? _processCharacterForThinkingTags(String character) {
    final (startTag, endTag) = thinkingTags;

    if (_insideThinkingTags) {
      // We're inside thinking tags, looking for the end tag
      _tagBuffer += character;

      // Check if buffer ends with the end tag
      if (_tagBuffer.endsWith(endTag)) {
        // Found end tag! Exit thinking mode
        _insideThinkingTags = false;
        _tagBuffer = '';
        _emitLog(ParseEvent(
          type: ParseEventType.thinkingTagEnd,
          propertyPath: '',
          message: 'Exited thinking tags',
        ));
        return null; // Don't process the tag characters
      }

      // Trim buffer to avoid unbounded growth - only keep enough to detect end tag
      if (_tagBuffer.length > endTag.length * 2) {
        _tagBuffer = _tagBuffer.substring(_tagBuffer.length - endTag.length);
      }

      return null; // Skip this character, we're inside thinking tags
    } else {
      // We're outside thinking tags, looking for the start tag
      _tagBuffer += character;

      // Check if buffer ends with the start tag
      if (_tagBuffer.endsWith(startTag)) {
        // Found start tag! Enter thinking mode
        _insideThinkingTags = true;
        _tagBuffer = '';
        _emitLog(ParseEvent(
          type: ParseEventType.thinkingTagStart,
          propertyPath: '',
          message: 'Entered thinking tags',
        ));
        return null; // Don't process the tag characters
      }

      // If buffer is longer than start tag, flush old characters for processing
      if (_tagBuffer.length > startTag.length) {
        // Trim buffer to keep only what might be part of start tag
        _tagBuffer = _tagBuffer.substring(_tagBuffer.length - startTag.length);

        // These characters are definitely not part of a start tag, process them
        return character;
      }

      // Buffer still building up, check if it could be a prefix of start tag
      if (startTag.startsWith(_tagBuffer)) {
        // Could still be building toward start tag, keep buffering
        return null;
      } else {
        // Buffer doesn't match start tag prefix, process buffered chars
        // For simplicity, since we're char-by-char, we clear buffer and pass through
        _tagBuffer = '';
        return character;
      }
    }
  }

  /// Detects potential thinking tags when skipThoughts is disabled.
  ///
  /// This is used to provide helpful error messages suggesting that the user
  /// might want to enable skipThoughts if the input contains thinking tags.
  void _detectPotentialThinkingTags(String character) {
    // Only detect before root delegate is established
    if (_rootDelegate != null) return;

    final (startTag, _) = thinkingTags;

    _potentialTagBuffer += character;

    // Check if buffer ends with the start tag
    if (_potentialTagBuffer.endsWith(startTag)) {
      _sawPotentialThinkingTags = true;
      _potentialTagBuffer = '';
      return;
    }

    // Trim buffer to prevent unbounded growth
    if (_potentialTagBuffer.length > startTag.length) {
      _potentialTagBuffer = _potentialTagBuffer
          .substring(_potentialTagBuffer.length - startTag.length);
    }
  }

  // PropertyGetterMixin implementation
  @override
  String buildPropertyPath(String key) {
    // For root-level JsonStreamParser, just return the key directly
    return key;
  }

  @override
  JsonStreamParserController get parserController => _controller;

  /// Disposes the parser and cleans up all resources.
  ///
  /// This method:
  /// - Cancels the stream subscription
  /// - Closes all stream controllers
  /// - Completes any pending futures with an error if they haven't completed
  /// - Clears all internal state
  ///
  /// After calling dispose(), this parser instance should not be used.
  Future<void> dispose() async {
    if (_isDisposed) {
      return;
    }

    _isDisposed = true;

    // Emit rootComplete if root was parsed
    if (_rootDelegate != null && _rootDelegate!.isDone) {
      _emitLog(ParseEvent(
        type: ParseEventType.rootComplete,
        propertyPath: '',
        message: 'Root JSON object/array completed parsing',
      ));
    }

    // Emit disposed event
    _emitLog(ParseEvent(
      type: ParseEventType.disposed,
      propertyPath: '',
      message: 'Parser disposed',
    ));

    // Cancel the stream subscription
    await _streamSubscription.cancel();

    // Close all stream controllers and complete pending futures with errors
    for (final controller in _propertyControllers.values) {
      if (controller is StringPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
        if (!controller.streamController.isClosed) {
          await controller.streamController.close();
        }
      } else if (controller is NumberPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
        if (!controller.streamController.isClosed) {
          await controller.streamController.close();
        }
      } else if (controller is BooleanPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
        if (!controller.streamController.isClosed) {
          await controller.streamController.close();
        }
      } else if (controller is NullPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
        if (!controller.streamController.isClosed) {
          await controller.streamController.close();
        }
      } else if (controller is MapPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
      } else if (controller is ListPropertyStreamController) {
        if (!controller.completer.isCompleted) {
          controller.completer.completeError(
            StateError('Parser was disposed before property completed'),
          );
        }
      }
    }

    // Clear all state
    _propertyControllers.clear();
    _rootDelegate = null;
  }
}

class JsonStreamParserController {
  JsonStreamParserController({
    required this.addPropertyChunk,
    required this.getPropertyStreamController,
    required this.getPropertyStream,
    required this.emitLog,
  });

  final void Function<T>({required String propertyPath, required T chunk})
      addPropertyChunk;

  PropertyStreamController Function(String propertyPath)
      getPropertyStreamController;

  /// Gets a PropertyStream for the given path, creating the controller if needed.
  /// The type parameter indicates what kind of stream to create.
  final PropertyStream Function(String propertyPath, Type streamType)
      getPropertyStream;

  /// Emits a log event to the parser's log callback.
  final void Function(ParseEvent event) emitLog;
}
