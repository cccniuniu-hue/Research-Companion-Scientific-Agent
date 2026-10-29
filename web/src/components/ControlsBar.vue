<template>
  <div class="controls-bar">
    <button type="button" class="rag-toggle-wrap" disabled title="当前页面未配置管理凭证，暂不能启用共享知识库">
      <div class="toggle-pill"></div>
      <span class="toggle-label">知识库（需管理权限）</span>
    </button>

    <div class="tools-btn-wrap">
      <button type="button" class="tools-btn" :class="{ active: skills.enabledCount > 0 }" :disabled="!auth.loggedIn" @click="$emit('open-skills')">
        <span>🧩</span>
        <span>Skill 广场</span>
        <span class="tool-count" v-if="skills.enabledCount > 0">{{ skills.enabledCount }}</span>
      </button>
    </div>

    <div class="controls-right" :class="chat.modeHint.cls">{{ chat.modeHint.text }}</div>
  </div>
</template>

<script setup>
import { useChat } from '../stores/chat'
import { useSkills } from '../stores/skills'
import { useAuth } from '../stores/auth'

defineEmits(['open-skills'])
const chat = useChat()
const skills = useSkills()
const auth = useAuth()
</script>

<style scoped>
.rag-toggle-wrap { border: 0; background: transparent; color: var(--text2); font: inherit; }
.tools-btn { font: inherit; }
button:disabled { cursor: not-allowed; opacity: .7; }
button:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
.controls-bar { flex-wrap: wrap; }
.controls-right.hint-primary { color: var(--primary-d); }
.controls-right.hint-pink { color: var(--pink-d); }
.controls-right.hint-muted { color: var(--text3); }
</style>
