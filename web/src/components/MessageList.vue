<template>
  <div class="messages" ref="scroller" role="log" aria-label="对话消息" aria-live="polite" aria-relevant="additions" tabindex="0" @scroll="onScroll">
    <div v-if="!sessions.messages.length" class="welcome">
      <div class="welcome-icon">◈</div>
      <div class="welcome-title">开始你的研究对话</div>
      <div class="welcome-desc">登录后可提问、整理思路，或使用已启用的 Skill。共享文档与知识库需要管理权限。</div>
    </div>
    <MessageBubble v-for="(m, i) in sessions.messages" :key="i" :msg="m" />
  </div>
</template>

<script setup>
import { ref, watch, nextTick } from 'vue'
import { useSessions } from '../stores/sessions'
import MessageBubble from './MessageBubble.vue'

const sessions = useSessions()
const scroller = ref(null)
const followLatest = ref(true)

function onScroll() {
  const el = scroller.value
  if (el) followLatest.value = el.scrollHeight - el.scrollTop - el.clientHeight < 48
}
async function scrollLatest() {
  await nextTick()
  if (followLatest.value && scroller.value) scroller.value.scrollTop = scroller.value.scrollHeight
}
// 阅读历史时不抢走滚动位置；切换会话后显示最新消息。
watch(() => sessions.messages, scrollLatest, { deep: true, flush: 'post' })
watch(() => sessions.currentId, () => { followLatest.value = true; scrollLatest() }, { immediate: true, flush: 'post' })
</script>

<style scoped>
.messages:focus-visible { outline: 2px solid var(--primary-d); outline-offset: -2px; }
</style>
