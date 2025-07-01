import 'package:flutter/material.dart';
import 'package:turso_dart/turso_dart.dart';

Future<void> main() async {
  final c = LibsqlClient.memory();
  await c.connect();
  final res = await c.query("SELECT 1");
  print(res);
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('flutter_rust_bridge quickstart')),
      ),
    );
  }
}
