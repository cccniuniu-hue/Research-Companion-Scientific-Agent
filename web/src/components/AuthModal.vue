<template>
  <div class="auth-overlay show" @keydown="onKey">
    <form class="auth-card" role="dialog" aria-modal="true" aria-labelledby="auth-title" ref="card" @submit.prevent="submit">
      <div class="auth-title" id="auth-title">Research Companion</div>
      <div class="auth-sub">{{ auth.mode === 'login' ? '登录以使用专属记忆' : '创建一个新账号' }}</div>

      <div class="auth-tabs">
        <button type="button" class="auth-tab" :aria-pressed="auth.mode === 'login'" :class="{ active: auth.mode === 'login' }" :disabled="auth.submitting" @click="auth.setMode('login')">登录</button>
        <button type="button" class="auth-tab" :aria-pressed="auth.mode === 'register'" :class="{ active: auth.mode === 'register' }" :disabled="auth.submitting" @click="auth.setMode('register')">注册</button>
      </div>

      <div class="auth-field">
        <label for="auth-username">用户名</label>
        <input id="auth-username" ref="usernameInput" type="text" v-model="username" autocomplete="username" placeholder="3–32 字符" required :disabled="auth.submitting" />
      </div>
      <div class="auth-field">
        <label for="auth-password">密码</label>
        <input id="auth-password" type="password" v-model="password" :autocomplete="auth.mode === 'register' ? 'new-password' : 'current-password'" placeholder="8–64 字节" required :disabled="auth.submitting" />
      </div>

      <div class="auth-error" role="alert" :class="{ show: !!auth.error }">{{ auth.error }}</div>

      <button type="submit" class="auth-submit" :disabled="auth.submitting">
        {{ auth.submitting ? '处理中...' : (auth.mode === 'login' ? '登录' : '注册') }}
      </button>
    </form>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { useAuth } from '../stores/auth'

const auth = useAuth()
const username = ref('')
const password = ref('')
const card = ref(null)
const usernameInput = ref(null)
let previousFocus
onMounted(() => {
  previousFocus = typeof document !== 'undefined' ? document.activeElement : null
  usernameInput.value?.focus()
})
onBeforeUnmount(() => previousFocus?.focus())

function onKey(e) {
  if (e.key === 'Enter' && (e.isComposing || e.keyCode === 229)) e.preventDefault()
  if (e.key !== 'Tab' || typeof document === 'undefined') return
  const fields = [...card.value.querySelectorAll('button:not(:disabled), input:not(:disabled)')]
  const first = fields[0], last = fields.at(-1)
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus() }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus() }
}

async function submit() {
  if (auth.submitting) return
  const ok = await auth.submit(username.value.trim(), password.value)
  if (ok) password.value = ''
}
</script>

<style scoped>
.auth-tab { border: 0; background: transparent; font: inherit; }
button:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
button:disabled { cursor: wait; }
.auth-field input { font-size: 16px; }
</style>
