// Stream engine bytes instead of retaining full decompressed WASM copies on mobile.
globalThis.ReclaimerWasmLoader = {
  async response(manifest, options = {}) {
    const request = options.fetch || globalThis.fetch;
    let stream;
    let activeReader;
    const compressed = options.decompress !== false && typeof DecompressionStream !== 'undefined';
    if (compressed) {
      const result = await request(manifest.gzip);
      if (!result.ok || !result.body) throw new Error('引擎资源加载失败，请刷新后重试。');
      let received = 0;
      const total = Number(result.headers.get('content-length'));
      const progress = new TransformStream({ transform(chunk, controller) {
        received += chunk.byteLength;
        options.progress?.(received, total);
        controller.enqueue(chunk);
      }});
      stream = result.body.pipeThrough(progress).pipeThrough(new DecompressionStream('gzip'));
    } else {
      // Sequential fallback retains at most the current chunk rather than all chunks + a combined copy.
      let index = 0;
      let received = 0;
      stream = new ReadableStream({
        async pull(controller) {
          try {
           while (true) {
            if (!activeReader) {
              if (index >= manifest.chunks.length) { controller.close(); return; }
              const result = await request(manifest.chunks[index++]);
              if (!result.ok || !result.body) throw new Error('引擎资源加载失败，请刷新后重试。');
              activeReader = result.body.getReader();
            }
            const item = await activeReader.read();
            if (item.done) { activeReader.releaseLock(); activeReader = null; continue; }
            received += item.value.byteLength;
            options.progress?.(received, manifest.size);
            controller.enqueue(item.value);
            return;
           }
          } catch (error) { controller.error(error); }
        },
        cancel(reason) { return activeReader?.cancel(reason); }
      });
    }
    let decoded = 0;
    const checked = stream.pipeThrough(new TransformStream({
      transform(chunk, controller) {
        decoded += chunk.byteLength;
        if (decoded > manifest.size) throw new Error('引擎资源大小不正确。');
        controller.enqueue(chunk);
      },
      flush() { if (decoded !== manifest.size) throw new Error('引擎资源不完整，请刷新后重试。'); }
    }));
    return new Response(checked, { headers: {
      'Content-Type': 'application/wasm', 'Content-Length': String(manifest.size)
    }});
  }
};
