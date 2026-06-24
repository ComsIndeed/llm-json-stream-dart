import 'dart:async';
import 'package:test/test.dart';
import 'package:llm_json_stream/llm_json_stream.dart';

void main() {
  group('Idempotency & Convenience API Tests', () {
    test('JsonStreamParser root convenience API gets correct values for maps', () async {
      final jsonStream = Stream.value('{"name": "Alice", "tags": ["admin", "user"]}');
      final parser = JsonStreamParser(jsonStream);

      // Verify convenience getters for Map
      expect(parser.asMap, isA<MapPropertyStream>());
      expect(parser.stream, isA<Stream<Map<String, dynamic>>>());
      expect(parser.future, isA<Future<Map<String, dynamic>>>());

      final map = await parser.future;
      expect(map['name'], equals('Alice'));
    });

    test('JsonStreamParser root convenience API gets correct values for lists', () async {
      final jsonStream = Stream.value('["admin", "user"]');
      final parser = JsonStreamParser(jsonStream);

      // Verify convenience getters for List
      expect(parser.asList(), isA<ListPropertyStream>());
      
      final list = await parser.asList().future;
      expect(list, equals(['admin', 'user']));
    });

    test('Stream instances are reference identical (idempotent)', () {
      final parser = JsonStreamParser(Stream.value('{"name": "Alice", "tags": ["admin"]}'));

      final mapStream = parser.asMap;
      final listStream = parser.getListProperty('tags');
      final stringStream = parser.getStringProperty('name');

      // Test MapPropertyStream idempotency
      final ms1 = mapStream.stream;
      final ms2 = mapStream.stream;
      expect(identical(ms1, ms2), isTrue, reason: 'MapPropertyStream.stream must be reference identical');

      // Test ListPropertyStream idempotency
      final ls1 = listStream.stream;
      final ls2 = listStream.stream;
      expect(identical(ls1, ls2), isTrue, reason: 'ListPropertyStream.stream must be reference identical');

      // Test StringPropertyStream idempotency
      final ss1 = stringStream.stream;
      final ss2 = stringStream.stream;
      expect(identical(ss1, ss2), isTrue, reason: 'StringPropertyStream.stream must be reference identical');
    });

    test('ReplayableBroadcastStream replays to multiple distinct subscribers correctly', () async {
      final controller = StreamController<String>();
      final parser = JsonStreamParser(controller.stream);

      final nameStream = parser.getStringProperty('name').stream;

      final events1 = <String>[];
      final events2 = <String>[];

      // Subscriber 1 listens early
      final sub1 = nameStream.listen(events1.add);

      controller.add('{"name": "Al');
      await Future.delayed(Duration(milliseconds: 10));
      expect(events1, contains('Al'));

      // Subscriber 2 listens late, should receive replayed buffer chunk
      final sub2 = nameStream.listen(events2.add);
      await Future.delayed(Duration(milliseconds: 10));
      expect(events2, contains('Al'));

      // Add remaining chunk
      controller.add('ice"}');
      await Future.delayed(Duration(milliseconds: 10));

      expect(events1, equals(['Al', 'ice']));
      expect(events2, equals(['Al', 'ice']));

      await sub1.cancel();
      await sub2.cancel();
      await controller.close();
    });
  });
}
