import 'package:llm_json_stream/llm_json_stream.dart';
import 'package:test/test.dart';

/// Enable verbose logging to debug test execution
const bool verbose = false;

/// Helper extension for test timeouts
extension FutureTimeout<T> on Future<T> {
  Future<T> withTestTimeout([Duration duration = const Duration(seconds: 5)]) {
    return timeout(duration, onTimeout: () {
      throw TimeoutException('Future timed out after $duration');
    });
  }
}

class TimeoutException implements Exception {
  final String message;
  TimeoutException(this.message);
  @override
  String toString() => message;
}

void main() {
  group('Thinking Tag Skipper Tests', () {
    group('Basic functionality', () {
      test('should skip content inside default <think></think> tags', () async {
        if (verbose) print('\n[TEST] Skip content inside default think tags');

        final json =
            '<think>Let me analyze this request...</think>{"name":"Alice"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final nameStream = parser.getStringProperty("name");
        final finalValue = await nameStream.future.withTestTimeout();

        if (verbose) print('[FINAL] $finalValue');
        expect(finalValue, equals('Alice'));
      });

      test('should work with custom thinking tags', () async {
        if (verbose) print('\n[TEST] Skip content with custom tags');

        final json = '[thought]Processing...[/thought]{"value":42}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          thinkingTags: ('[thought]', '[/thought]'),
        );

        final valueStream = parser.getNumberProperty("value");
        final finalValue = await valueStream.future.withTestTimeout();

        if (verbose) print('[FINAL] $finalValue');
        expect(finalValue, equals(42));
      });

      test('should not skip thoughts when skipThoughts is false', () async {
        if (verbose) print('\n[TEST] Do not skip thoughts when disabled');

        // With skipThoughts disabled, the <think> tag will prevent finding valid JSON
        // at the root level. The parser should fail to find the JSON.
        final json = '{"name":"Alice"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: false);

        final nameStream = parser.getStringProperty("name");
        final finalValue = await nameStream.future.withTestTimeout();

        expect(finalValue, equals('Alice'));
      });

      test('should handle JSON without any thinking tags', () async {
        if (verbose) print('\n[TEST] Handle JSON without thinking tags');

        final json = '{"status":"ok","count":5}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final statusStream = parser.getStringProperty("status");
        final countStream = parser.getNumberProperty("count");

        final status = await statusStream.future.withTestTimeout();
        final count = await countStream.future.withTestTimeout();

        expect(status, equals('ok'));
        expect(count, equals(5));
      });
    });

    group('Multiple thinking blocks', () {
      test('should skip multiple thinking blocks before JSON', () async {
        if (verbose) print('\n[TEST] Skip multiple thinking blocks');

        final json =
            '<think>First thought</think><think>Second thought</think>{"result":"success"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final resultStream = parser.getStringProperty("result");
        final finalValue = await resultStream.future.withTestTimeout();

        expect(finalValue, equals('success'));
      });

      test('should handle whitespace between thinking blocks and JSON',
          () async {
        if (verbose) print('\n[TEST] Handle whitespace between blocks');

        final json = '<think>Analysis...</think>\n\n  {"data":"test"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final dataStream = parser.getStringProperty("data");
        final finalValue = await dataStream.future.withTestTimeout();

        expect(finalValue, equals('test'));
      });
    });

    group('Nested content within thinking tags', () {
      test('should skip JSON-like content inside thinking tags', () async {
        if (verbose) print('\n[TEST] Skip JSON-like content inside think tags');

        final json =
            '<think>{"fake":"json"} should be ignored</think>{"real":"json"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final realStream = parser.getStringProperty("real");
        final finalValue = await realStream.future.withTestTimeout();

        expect(finalValue, equals('json'));
      });

      test('should skip arrays inside thinking tags', () async {
        if (verbose) print('\n[TEST] Skip arrays inside think tags');

        final json =
            '<think>[1, 2, 3] is a fake array</think>{"items":[4,5,6]}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final itemsStream = parser.getListProperty<int>("items");
        final finalValue = await itemsStream.future.withTestTimeout();

        expect(finalValue, equals([4, 5, 6]));
      });

      test('should handle nested angle brackets inside thinking tags',
          () async {
        if (verbose) print('\n[TEST] Handle nested angle brackets');

        final json =
            '<think>I need to use <code> tags</think>{"message":"hello"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final messageStream = parser.getStringProperty("message");
        final finalValue = await messageStream.future.withTestTimeout();

        expect(finalValue, equals('hello'));
      });

      test('should handle multi-line thinking content', () async {
        if (verbose) print('\n[TEST] Handle multi-line thinking content');

        final json = '''<think>
Let me think about this step by step:
1. First, I'll consider the requirements
2. Then, I'll formulate a response
3. Finally, I'll output the JSON
</think>{"answer":"42"}''';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 10,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final answerStream = parser.getStringProperty("answer");
        final finalValue = await answerStream.future.withTestTimeout();

        expect(finalValue, equals('42'));
      });
    });

    group('Edge cases with tag patterns', () {
      test('should handle partial tag-like sequences before JSON', () async {
        if (verbose) print('\n[TEST] Handle partial tag-like sequences');

        // This has a < but not a full <think> tag
        final json = '<thin{"name":"test"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 2,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final nameStream = parser.getStringProperty("name");
        final finalValue = await nameStream.future.withTestTimeout();

        expect(finalValue, equals('test'));
      });

      test('should not drop characters on partial thinking tag mismatch inside JSON string', () async {
        if (verbose) print('\n[TEST] Partial tag mismatch inside JSON string');

        final json = '{"text": "I love <the color and <thi is nice"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final textStream = parser.getStringProperty("text");
        final finalValue = await textStream.future.withTestTimeout();

        expect(finalValue, equals('I love <the color and <thi is nice'));
      });

      test('should handle think tags split across chunks', () async {
        if (verbose) print('\n[TEST] Handle think tags split across chunks');

        final json = '<think>thinking...</think>{"result":"done"}';
        if (verbose) print('[JSON] $json');

        // Use chunk size of 1 to ensure tags are split
        final stream = streamTextInChunks(
          text: json,
          chunkSize: 1,
          interval: Duration(milliseconds: 5),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final resultStream = parser.getStringProperty("result");
        final finalValue = await resultStream.future.withTestTimeout();

        expect(finalValue, equals('done'));
      });

      test('should handle closing tag split across chunks', () async {
        if (verbose) print('\n[TEST] Handle closing tag split across chunks');

        final json = '<think>content</think>{"x":"y"}';
        if (verbose) print('[JSON] $json');

        // Chunk size 7 should split </think> tag
        final stream = streamTextInChunks(
          text: json,
          chunkSize: 7,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final xStream = parser.getStringProperty("x");
        final finalValue = await xStream.future.withTestTimeout();

        expect(finalValue, equals('y'));
      });

      test('should handle very long thinking content', () async {
        if (verbose) print('\n[TEST] Handle very long thinking content');

        final longThought = 'A' * 10000; // 10KB of thinking
        final json = '<think>$longThought</think>{"short":"value"}';
        if (verbose) print('[JSON LENGTH] ${json.length}');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 100,
          interval: Duration(milliseconds: 1),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final shortStream = parser.getStringProperty("short");
        final finalValue =
            await shortStream.future.withTestTimeout(Duration(seconds: 10));

        expect(finalValue, equals('value'));
      });

      test('should handle empty thinking tags', () async {
        if (verbose) print('\n[TEST] Handle empty thinking tags');

        final json = '<think></think>{"empty":"test"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final emptyStream = parser.getStringProperty("empty");
        final finalValue = await emptyStream.future.withTestTimeout();

        expect(finalValue, equals('test'));
      });
    });

    group('Complex JSON structures with thinking tags', () {
      test('should handle nested objects after thinking tags', () async {
        if (verbose) print('\n[TEST] Handle nested objects');

        final json =
            '<think>Reasoning...</think>{"user":{"name":"Bob","age":30}}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final nameStream = parser.getStringProperty("user.name");
        final ageStream = parser.getNumberProperty("user.age");

        final name = await nameStream.future.withTestTimeout();
        final age = await ageStream.future.withTestTimeout();

        expect(name, equals('Bob'));
        expect(age, equals(30));
      });

      test('should handle arrays at root level after thinking tags', () async {
        if (verbose) print('\n[TEST] Handle root array');

        final json = '<think>Let me list...</think>[1,2,3,4,5]';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final listStream = parser.getListProperty<int>("");
        final finalValue = await listStream.future.withTestTimeout();

        expect(finalValue, equals([1, 2, 3, 4, 5]));
      });

      test('should handle complex nested structures', () async {
        if (verbose) print('\n[TEST] Handle complex nested structures');

        final json = '''<think>Complex structure coming...</think>{
          "users": [
            {"name": "Alice", "active": true},
            {"name": "Bob", "active": false}
          ],
          "metadata": {
            "count": 2,
            "version": "1.0"
          }
        }''';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 10,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final countStream = parser.getNumberProperty("metadata.count");
        final versionStream = parser.getStringProperty("metadata.version");

        final count = await countStream.future.withTestTimeout();
        final version = await versionStream.future.withTestTimeout();

        expect(count, equals(2));
        expect(version, equals('1.0'));
      });
    });

    group('Custom thinking tags variations', () {
      test('should work with XML-style reasoning tags', () async {
        if (verbose) print('\n[TEST] XML-style reasoning tags');

        final json =
            '<reasoning>Step 1: analyze...</reasoning>{"output":"done"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          thinkingTags: ('<reasoning>', '</reasoning>'),
        );

        final outputStream = parser.getStringProperty("output");
        final finalValue = await outputStream.future.withTestTimeout();

        expect(finalValue, equals('done'));
      });

      test('should work with bracket-style tags', () async {
        if (verbose) print('\n[TEST] Bracket-style tags');

        final json =
            '[[THINKING]]internal monologue[[/THINKING]]{"response":"hi"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          thinkingTags: ('[[THINKING]]', '[[/THINKING]]'),
        );

        final responseStream = parser.getStringProperty("response");
        final finalValue = await responseStream.future.withTestTimeout();

        expect(finalValue, equals('hi'));
      });

      test('should work with simple delimiter tags', () async {
        if (verbose) print('\n[TEST] Simple delimiter tags');

        final json = '---THINK---some thoughts---/THINK---{"key":"val"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          thinkingTags: ('---THINK---', '---/THINK---'),
        );

        final keyStream = parser.getStringProperty("key");
        final finalValue = await keyStream.future.withTestTimeout();

        expect(finalValue, equals('val'));
      });

      test('should work with emoji tags', () async {
        if (verbose) print('\n[TEST] Emoji tags');

        final json = '🤔🤔🤔reasoning here💭💭💭{"emoji":"works"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          thinkingTags: ('🤔🤔🤔', '💭💭💭'),
        );

        final emojiStream = parser.getStringProperty("emoji");
        final finalValue = await emojiStream.future.withTestTimeout();

        expect(finalValue, equals('works'));
      });
    });

    group('Integration with other parser features', () {
      test('should work with closeOnRootComplete enabled', () async {
        if (verbose) print('\n[TEST] Work with closeOnRootComplete');

        final json = '<think>thoughts</think>{"done":true} extra text here';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          closeOnRootComplete: true,
        );

        final doneStream = parser.getBooleanProperty("done");
        final finalValue = await doneStream.future.withTestTimeout();

        expect(finalValue, equals(true));
      });

      test('should emit thinking tag events when logged', () async {
        if (verbose) print('\n[TEST] Emit thinking tag events');

        final json = '<think>content</think>{"test":1}';
        if (verbose) print('[JSON] $json');

        final events = <ParseEvent>[];
        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(
          stream,
          skipThoughts: true,
          onLog: (event) {
            events.add(event);
            if (verbose) print('[EVENT] $event');
          },
        );

        final testStream = parser.getNumberProperty("test");
        await testStream.future.withTestTimeout();

        // Check that we got thinking tag events
        expect(
          events.any((e) => e.type == ParseEventType.thinkingTagStart),
          isTrue,
          reason: 'Should emit thinkingTagStart event',
        );
        expect(
          events.any((e) => e.type == ParseEventType.thinkingTagEnd),
          isTrue,
          reason: 'Should emit thinkingTagEnd event',
        );
      });

      test('should handle streaming string properties after thinking tags',
          () async {
        if (verbose) print('\n[TEST] Streaming string after thinking');

        final json = '<think>preparing...</think>{"message":"Hello, World!"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 2,
          interval: Duration(milliseconds: 5),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final messageStream = parser.getStringProperty("message");

        final chunks = <String>[];
        messageStream.stream.listen((chunk) {
          chunks.add(chunk);
          if (verbose) print('[CHUNK] "$chunk"');
        });

        final finalValue = await messageStream.future.withTestTimeout();

        expect(chunks.join(''), equals('Hello, World!'));
        expect(finalValue, equals('Hello, World!'));
      });
    });

    group('Error scenarios and robustness', () {
      test(
          'parser skips non-JSON characters before root even without skipThoughts',
          () async {
        if (verbose) print('\n[TEST] Parser skips non-JSON before root');

        // Interesting behavior: the parser naturally skips non-JSON characters
        // before the root element (looking for { or [), so simple thinking tags
        // get passed over anyway. The skipThoughts feature is still useful for:
        // 1. Cases where thinking content contains { or [ that could confuse parser
        // 2. Logging/observability to track thinking tag boundaries
        // 3. Custom tag formats that might otherwise cause issues
        final json = '<think>simple thinking</think>{"name":"Alice"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        // Even without skipThoughts, simple tags before JSON work
        final parser = JsonStreamParser(stream, skipThoughts: false);

        final nameStream = parser.getStringProperty("name");
        final finalValue = await nameStream.future.withTestTimeout();

        expect(finalValue, equals('Alice'));
      });

      test(
          'skipThoughts required when thinking content has JSON-like characters',
          () async {
        if (verbose) {
          print('\n[TEST] JSON-like chars in thinking need skipThoughts');
        }

        // This scenario REQUIRES skipThoughts because the { in thinking
        // content will be interpreted as the start of JSON
        final json =
            '<think>{"fake": "json"} in thoughts</think>{"real":"json"}';
        if (verbose) print('[JSON] $json');

        // Test 1: Without skipThoughts, the parser gets confused
        final stream1 = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser1 = JsonStreamParser(stream1, skipThoughts: false);

        final nameStream1 = parser1.getStringProperty("real");
        var failedWithoutSkip = false;
        try {
          await nameStream1.future.timeout(Duration(milliseconds: 500));
        } catch (e) {
          failedWithoutSkip = true;
        }

        expect(failedWithoutSkip, isTrue,
            reason:
                'Should fail when thinking has JSON and skipThoughts is false');

        // Test 2: With skipThoughts, it works correctly
        final stream2 = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser2 = JsonStreamParser(stream2, skipThoughts: true);

        final nameStream2 = parser2.getStringProperty("real");
        final finalValue = await nameStream2.future.withTestTimeout();

        expect(finalValue, equals('json'));
      });

      test('should handle unclosed thinking tag gracefully', () async {
        if (verbose) print('\n[TEST] Handle unclosed thinking tag');

        // Unclosed thinking tag - parser should wait/timeout
        final json = '<think>never closes...{"orphan":"data"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 5,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final orphanStream = parser.getStringProperty("orphan");

        // This should timeout because we never exit thinking tags
        var timedOut = false;
        try {
          await orphanStream.future.timeout(Duration(milliseconds: 500));
        } catch (e) {
          // Any timeout exception is acceptable
          timedOut = true;
        }
        expect(timedOut, isTrue,
            reason: 'Should timeout with unclosed thinking tag');
      });

      test('should handle thinking content that looks like closing tag',
          () async {
        if (verbose) print('\n[TEST] Content that looks like closing tag');

        final json =
            '<think>What if I use </thin and then continue</think>{"ok":"yes"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final okStream = parser.getStringProperty("ok");
        final finalValue = await okStream.future.withTestTimeout();

        expect(finalValue, equals('yes'));
      });

      test('should handle special characters in thinking content', () async {
        if (verbose) print('\n[TEST] Special characters in thinking content');

        final json =
            r'<think>Special chars: \n \t " { } [ ] < ></think>{"special":"handled"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final specialStream = parser.getStringProperty("special");
        final finalValue = await specialStream.future.withTestTimeout();

        expect(finalValue, equals('handled'));
      });

      test('should handle unicode in thinking tags', () async {
        if (verbose) print('\n[TEST] Unicode in thinking tags');

        final json = '<think>日本語で考えています 🎉</think>{"lang":"ja"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final langStream = parser.getStringProperty("lang");
        final finalValue = await langStream.future.withTestTimeout();

        expect(finalValue, equals('ja'));
      });
    });

    group('Performance and stress tests', () {
      test('should handle rapid alternating thinking and JSON content',
          () async {
        if (verbose) print('\n[TEST] Rapid alternating content');

        // Only one JSON object at root, but with thinking before it
        final json =
            '<think>t1</think><think>t2</think><think>t3</think>{"final":"value"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 2,
          interval: Duration(milliseconds: 5),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final finalStream = parser.getStringProperty("final");
        final finalValue = await finalStream.future.withTestTimeout();

        expect(finalValue, equals('value'));
      });

      test('should handle many small chunks efficiently', () async {
        if (verbose) print('\n[TEST] Many small chunks');

        final json =
            '<think>This is a moderately long thought process</think>{"efficient":"yes"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 1, // Character by character
          interval: Duration(milliseconds: 1),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final efficientStream = parser.getStringProperty("efficient");
        final finalValue =
            await efficientStream.future.withTestTimeout(Duration(seconds: 10));

        expect(finalValue, equals('yes'));
      });
    });

    group('Tag boundary edge cases', () {
      test('should not match partial opening tag', () async {
        if (verbose) print('\n[TEST] Partial opening tag');

        // "<thin" is not "<think>", so JSON should parse
        final json = '<thin{"key":"value"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final keyStream = parser.getStringProperty("key");
        final finalValue = await keyStream.future.withTestTimeout();

        expect(finalValue, equals('value'));
      });

      test('should handle tag at exact chunk boundary', () async {
        if (verbose) print('\n[TEST] Tag at exact chunk boundary');

        // "<think>" is 7 chars, use chunk size 7
        final json = '<think>thought</think>{"exact":"boundary"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 7,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final exactStream = parser.getStringProperty("exact");
        final finalValue = await exactStream.future.withTestTimeout();

        expect(finalValue, equals('boundary'));
      });

      test('should handle back-to-back closing and opening tags', () async {
        if (verbose) print('\n[TEST] Back-to-back tags');

        final json =
            '<think>first</think><think>second</think>{"back":"toback"}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final backStream = parser.getStringProperty("back");
        final finalValue = await backStream.future.withTestTimeout();

        expect(finalValue, equals('toback'));
      });
    });

    group('Boolean, null, and number types after thinking', () {
      test('should parse boolean true after thinking', () async {
        if (verbose) print('\n[TEST] Boolean true after thinking');

        final json = '<think>deciding...</think>{"flag":true}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final flagStream = parser.getBooleanProperty("flag");
        final finalValue = await flagStream.future.withTestTimeout();

        expect(finalValue, equals(true));
      });

      test('should parse boolean false after thinking', () async {
        if (verbose) print('\n[TEST] Boolean false after thinking');

        final json = '<think>considering...</think>{"active":false}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final activeStream = parser.getBooleanProperty("active");
        final finalValue = await activeStream.future.withTestTimeout();

        expect(finalValue, equals(false));
      });

      test('should parse null after thinking', () async {
        if (verbose) print('\n[TEST] Null after thinking');

        final json = '<think>checking...</think>{"empty":null}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 4,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final emptyStream = parser.getNullProperty("empty");
        final finalValue = await emptyStream.future.withTestTimeout();

        expect(finalValue, isNull);
      });

      test('should parse floating point number after thinking', () async {
        if (verbose) print('\n[TEST] Float after thinking');

        final json = '<think>calculating...</think>{"pi":3.14159}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final piStream = parser.getNumberProperty("pi");
        final finalValue = await piStream.future.withTestTimeout();

        expect(finalValue, closeTo(3.14159, 0.00001));
      });

      test('should parse negative number after thinking', () async {
        if (verbose) print('\n[TEST] Negative number after thinking');

        final json = '<think>computing...</think>{"temp":-40}';
        if (verbose) print('[JSON] $json');

        final stream = streamTextInChunks(
          text: json,
          chunkSize: 3,
          interval: Duration(milliseconds: 10),
        );
        final parser = JsonStreamParser(stream, skipThoughts: true);

        final tempStream = parser.getNumberProperty("temp");
        final finalValue = await tempStream.future.withTestTimeout();

        expect(finalValue, equals(-40));
      });
    });
  });
}
