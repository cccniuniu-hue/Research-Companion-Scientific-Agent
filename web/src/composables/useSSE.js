// 只派发由空行结束的 JSON 事件；断流尾部不能冒充 done。
export async function readSSE(resp, onEvent) {
  if (!resp.body) throw new Error('流式响应为空')
  const reader = resp.body.getReader()
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let buf = '', event = '', data = [], size = 0
  const limit = 1024 * 1024
  const line = (text) => {
    if (!text) {
      if (data.length) onEvent(event || 'message', JSON.parse(data.join('\n')))
      event = ''; data = []; size = 0
      return
    }
    size += text.length
    if (size > limit) throw new Error('SSE event too large')
    const colon = text.indexOf(':')
    const field = colon < 0 ? text : text.slice(0, colon)
    let value = colon < 0 ? '' : text.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') event = value
    if (field === 'data') data.push(value)
  }
  try {
    while (true) {
      const { done, value } = await reader.read()
      buf += done ? decoder.decode() : decoder.decode(value, { stream: true })
      let pos
      while ((pos = buf.search(/[\r\n]/)) >= 0) {
        if (!done && pos === buf.length - 1 && buf[pos] === '\r') break
        const width = buf[pos] === '\r' && buf[pos + 1] === '\n' ? 2 : 1
        line(buf.slice(0, pos))
        buf = buf.slice(pos + width)
      }
      if (buf.length + size > limit) throw new Error('SSE event too large')
      if (done) break
    }
  } finally {
    try { await reader.cancel() } catch { /* 保留原始读流/回调错误。 */ }
    reader.releaseLock()
  }
}
