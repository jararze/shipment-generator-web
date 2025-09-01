import { createApp } from 'vue'
import App from './App.vue'
import './assets/style.css'

const app = createApp(App)

// Global error handler
app.config.errorHandler = (err, vm, info) => {
  console.error('Global error:', err, info)
}

app.mount('#app')
