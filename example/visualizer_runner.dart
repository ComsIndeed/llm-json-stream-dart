import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:llm_json_stream/llm_json_stream.dart';

void main() async {
  // Set stdout to unbuffered to ensure real-time stream pipe communication
  stdout.write('');

  // 1. Read first line of stdin (properties config JSON)
  final configLine = stdin.readLineSync(encoding: utf8);
  if (configLine == null) return;

  Map<String, dynamic> config;
  try {
    config = jsonDecode(configLine) as Map<String, dynamic>;
  } catch (e) {
    print(jsonEncode({'event': 'status', 'state': 'error', 'message': 'Invalid config: $e'}));
    return;
  }

  // Set up character streaming from stdin
  final controller = StreamController<String>();
  
  // Transform standard input bytes into character chunks
  final stdinSub = stdin.transform(utf8.decoder).listen(
    (data) {
      for (var i = 0; i < data.length; i++) {
        controller.add(data[i]);
      }
    },
    onDone: () {
      controller.close();
    },
    onError: (Object e) {
      controller.addError(e);
    }
  );

  // Helper to convert Dart camelCase event types to match Python format
  String convertEventType(ParseEventType type) {
    switch (type) {
      case ParseEventType.propertyStart:
        return 'PROPERTY_START';
      case ParseEventType.propertyComplete:
        return 'PROPERTY_COMPLETE';
      case ParseEventType.stringChunk:
        return 'STRING_CHUNK';
      case ParseEventType.listElementStart:
        return 'LIST_ELEMENT_START';
      case ParseEventType.mapKeyDiscovered:
        return 'MAP_KEY_DISCOVERED';
      case ParseEventType.rootStart:
        return 'ROOT_START';
      case ParseEventType.rootComplete:
        return 'ROOT_COMPLETE';
      case ParseEventType.error:
        return 'ERROR';
      case ParseEventType.disposed:
        return 'DISPOSED';
      case ParseEventType.yapFiltered:
        return 'YAP_FILTERED';
      case ParseEventType.thinkingTagStart:
        return 'THINKING_TAG_START';
      case ParseEventType.thinkingTagEnd:
        return 'THINKING_TAG_END';
    }
  }

  // Initialize parser with event log callback
  final parser = JsonStreamParser(
    controller.stream,
    onLog: (event) {
      final eventMap = {
        'event': 'parser_event',
        'type': convertEventType(event.type),
        'propertyPath': event.propertyPath,
        'message': event.message,
        'data': event.data?.toString(),
      };
      print(jsonEncode(eventMap));
    },
  );

  // Pre-register all properties based on config
  config.forEach((path, type) {
    try {
      if (type == 'string') {
        parser.getStringProperty(path);
      } else if (type == 'number') {
        parser.getNumberProperty(path);
      } else if (type == 'boolean') {
        parser.getBooleanProperty(path);
      } else if (type == 'null') {
        parser.getNullProperty(path);
      } else if (type == 'object') {
        parser.getMapProperty(path);
      } else if (type == 'array') {
        parser.getListProperty(path);
      }
    } catch (e) {
      print(jsonEncode({'event': 'status', 'state': 'error', 'message': 'Failed to register $path: $e'}));
    }
  });

  // Keep process alive until stream is completely consumed and parser completes
  // In Dart, we can await a future to complete or wait for stream subscription to finish
  await controller.done;
  await parser.dispose();
  await stdinSub.cancel();
}
