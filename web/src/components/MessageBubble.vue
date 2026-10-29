<template>
  <div class="msg-row" :class="msg.role">
    <div class="avatar" :class="msg.role">{{ msg.role === 'user' ? '◇' : '◈' }}</div>
    <div class="msg-body">
      <div class="msg-name">{{ msg.role === 'user' ? '你' : 'Research Companion' }}</div>

      <!-- 用户消息：纯文本 -->
      <div v-if="msg.role === 'user'" class="bubble user">{{ msg.text }}</div>

      <!-- AI 消息：结构化渲染 -->
      <div v-else class="bubble ai">
        <div v-if="msg.memory" class="memory-note">
          <span class="memory-tag">🧠 记忆</span>
          <span class="memory-content">{{ msg.memory }}</span>
        </div>

        <ThinkPanel :msg="msg" />

        <div v-if="msg.interrupted" class="interrupted-badge">🛑 已中断</div>

        <div v-if="!msg.answer && msg.streaming" class="typing" role="status" aria-label="正在生成回复"><span></span><span></span><span></span></div>
        <div v-else class="answer-text" v-html="rendered"></div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { renderMarkdown } from '../utils/markdown'
import ThinkPanel from './ThinkPanel.vue'

const props = defineProps({ msg: { type: Object, required: true } })
// 旧会话中的 HTML 也必须经过转义，不能绕过 Markdown 的安全边界。
const rendered = computed(() => renderMarkdown(props.msg.answer || props.msg.html || ''))
</script>

<style scoped>
.bubble.user { white-space: pre-wrap; }
.msg-body { min-width: 0; }
.answer-text, .bubble.user, .memory-content { overflow-wrap: anywhere; }
</style>
