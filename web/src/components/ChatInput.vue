<template>
  <div class="input-area">
    <div class="input-row">
      <textarea class="input-box" v-model="text" ref="ta" rows="1" aria-label="消息内容" :disabled="!auth.loggedIn"
        placeholder="输入消息，Enter 发送，Shift+Enter 换行…"
        @keydown="onKey" @input="resize"></textarea>
      <button type="button" class="send-btn" :class="{ stop: chat.loading }" :aria-label="chat.loading ? '停止生成' : '发送消息'" :disabled="!chat.loading && (!auth.loggedIn || !text.trim())" @click="onButton">{{ chat.loading ? '■' : '➤' }}</button>
    </div>
    <div class="input-hint">
      <button type="button" class="hint-chip" :disabled="chat.loading || !auth.loggedIn" @click="quick('你是谁？')">你是谁</button>
      <button type="button" class="hint-chip" :disabled="chat.loading || !auth.loggedIn" @click="quick('现在几点？')">现在几点</button>
      <button type="button" class="hint-chip" :disabled="chat.loading || !auth.loggedIn" @click="quick('北京天气怎么样？')">北京天气</button>
      <button type="button" class="hint-chip" :disabled="chat.loading || !auth.loggedIn" @click="quick('我喜欢周杰伦的音乐')">记住偏好</button>
      <button type="button" class="hint-chip" :disabled="chat.loading || !auth.loggedIn" @click="quick('写一份关于人工智能发展趋势的报告')">生成报告</button>
    </div>
  </div>
</template>

<script setup>
import { ref, nextTick, watch } from 'vue'
import { useChat } from '../stores/chat'
import { useAuth } from '../stores/auth'
import { useSessions } from '../stores/sessions'

const chat = useChat()
const auth = useAuth()
const sessions = useSessions()
const text = ref('')
const ta = ref(null)
watch(() => auth.userId, () => { text.value = ''; nextTick(resize) }, { flush: 'sync' })

function resize() {
  const el = ta.value
  if (!el) return
  el.style.height = 'auto'
  el.style.height = Math.min(el.scrollHeight, 140) + 'px'
}
function onKey(e) {
  if (e.isComposing || e.keyCode === 229) return
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); if (!chat.loading) onSend() }
}
function onButton() { return chat.loading ? chat.stop() : onSend() }
async function onSend() {
  if (chat.loading || !auth.loggedIn || sessions.userId !== auth.userId) return
  const msg = text.value.trim()
  if (!msg) return
  text.value = ''
  // send 同步建立请求状态，避免等待 DOM 更新期间的重复提交。
  const pending = chat.send(msg)
  await nextTick(); resize()
  await pending
}
function quick(m) {
  if (chat.loading || !auth.loggedIn) return
  text.value = m
  return onSend()
}
</script>

<style scoped>
.input-box { min-width: 0; font-size: 16px; }
.hint-chip { font-family: inherit; }
button:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
.hint-chip:disabled { opacity: .6; cursor: not-allowed; }
@media (prefers-reduced-motion: reduce) { .send-btn.stop { animation: none; } }
</style>
