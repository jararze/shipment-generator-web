// src/composables/useApi.js
import { ref, reactive } from 'vue'
import axios from 'axios'

// Configurar axios
const api = axios.create({
  baseURL: 'http://localhost:8000',
  timeout: 300000, // 5 minutos para uploads grandes
})

// Interceptor para logging
api.interceptors.request.use(request => {
  console.log('🚀 API Request:', request.method?.toUpperCase(), request.url)
  return request
})

api.interceptors.response.use(
  response => {
    console.log('✅ API Response:', response.status, response.config.url)
    return response
  },
  error => {
    console.error('❌ API Error:', error.response?.status, error.config?.url, error.message)
    return Promise.reject(error)
  }
)

export function useShipmentApi() {
  const isLoading = ref(false)
  const error = ref(null)

  // Estado de trabajos
  const jobs = reactive(new Map())

  /**
   * Subir archivo y procesar
   */
  const uploadFile = async (file, availabilityFile = null, options = {}) => {
    try {
      isLoading.value = true
      error.value = null

      const formData = new FormData()
      formData.append('file', file)
      if (availabilityFile) {
        formData.append('availability_file', availabilityFile); // <-- AÑADIR EL SEGUNDO ARCHIVO
      }
      formData.append('use_planta_as_origen', options.usePlantaAsOrigen || false);
      formData.append('skip_placas', options.skipPlacas || false);

      const response = await api.post('/api/upload-file', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        },
        onUploadProgress: (progressEvent) => {
          const percentCompleted = Math.round(
            (progressEvent.loaded * 100) / progressEvent.total
          )
          console.log(`📤 Upload Progress: ${percentCompleted}%`)
        }
      })

      const jobData = response.data

      // Guardar trabajo en estado local
      jobs.set(jobData.job_id, {
        ...jobData,
        filename: file.name,
        fileSize: file.size,
        options
      })

      return jobData

    } catch (err) {
      const errorMsg = err.response?.data?.detail || err.message || 'Error desconocido'
      error.value = errorMsg
      throw new Error(errorMsg)
    } finally {
      isLoading.value = false
    }
  }

  /**
   * Obtener estado de trabajo
   */
  const getJobStatus = async (jobId) => {
    try {
      const response = await api.get(`/api/job/${jobId}`)
      const jobData = response.data

      // Actualizar en estado local
      if (jobs.has(jobId)) {
        const existingJob = jobs.get(jobId)
        jobs.set(jobId, { ...existingJob, ...jobData })
      } else {
        jobs.set(jobId, jobData)
      }

      return jobData
    } catch (err) {
      console.error('Error obteniendo estado del trabajo:', err)
      throw err
    }
  }

  /**
   * Polling automático de estado
   */
  const pollJobStatus = (jobId, interval = 2000) => {
    const pollInterval = setInterval(async () => {
      try {
        const job = await getJobStatus(jobId)

        // Detener polling si el trabajo terminó
        if (job.status === 'completed' || job.status === 'error') {
          clearInterval(pollInterval)
        }

      } catch (err) {
        console.error('Error en polling:', err)
        clearInterval(pollInterval)
      }
    }, interval)

    return pollInterval
  }

  /**
   * Descargar archivo
   */
  const downloadFile = async (filePath, desiredFilename) => {
    try {
      const fileName = desiredFilename || filePath.split('/').pop();

      const response = await api.get(`/api/download/${encodeURIComponent(filePath)}`, {
        responseType: 'blob'
      });

      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', fileName); // <-- USAR EL NUEVO NOMBRE
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);

      console.log(`📥 Archivo descargado como: ${fileName}`);

    } catch (err) {
      const errorMsg = err.response?.data?.detail || 'Error descargando archivo'
      throw new Error(errorMsg)
    }
  }

  /**
   * Listar todos los trabajos
   */
  const listJobs = async (limit = 50) => {
    try {
      const response = await api.get(`/api/jobs?limit=${limit}`)
      return response.data
    } catch (err) {
      console.error('Error listando trabajos:', err)
      throw err
    }
  }

  /**
   * Eliminar trabajo
   */
  const deleteJob = async (jobId) => {
    try {
      await api.delete(`/api/job/${jobId}`)
      jobs.delete(jobId)
      console.log(`🗑️ Trabajo eliminado: ${jobId}`)
    } catch (err) {
      console.error('Error eliminando trabajo:', err)
      throw err
    }
  }

  /**
   * Health check
   */
  const healthCheck = async () => {
    try {
      const response = await api.get('/api/health')
      return response.data
    } catch (err) {
      console.error('Health check falló:', err)
      throw err
    }
  }

  /**
   * Limpiar trabajos antiguos
   */
  const cleanupOldJobs = async (days = 7) => {
    try {
      const response = await api.post(`/api/cleanup?days=${days}`)
      return response.data
    } catch (err) {
      console.error('Error en limpieza:', err)
      throw err
    }
  }

  return {
    // Estado
    isLoading,
    error,
    jobs,

    // Métodos
    uploadFile,
    getJobStatus,
    pollJobStatus,
    downloadFile,
    listJobs,
    deleteJob,
    healthCheck,
    cleanupOldJobs
  }
}

// Composable para manejo de archivos
export function useFileHandling() {
  const validateFile = (file) => {
    const validExtensions = ['.xlsx', '.xlsm', '.xls']
    const maxSize = 50 * 1024 * 1024 // 50MB

    const errors = []

    if (!file) {
      errors.push('No se ha seleccionado ningún archivo')
      return { isValid: false, errors }
    }

    // Verificar extensión
    const hasValidExtension = validExtensions.some(ext =>
      file.name.toLowerCase().endsWith(ext)
    )

    if (!hasValidExtension) {
      errors.push(`Tipo de archivo no válido. Use: ${validExtensions.join(', ')}`)
    }

    // Verificar tamaño
    if (file.size > maxSize) {
      errors.push(`Archivo demasiado grande. Máximo: ${formatFileSize(maxSize)}`)
    }

    return {
      isValid: errors.length === 0,
      errors
    }
  }

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  const getFileIcon = (filename) => {
    const ext = filename.toLowerCase().split('.').pop()

    const iconMap = {
      'xlsx': '📊',
      'xlsm': '📊',
      'xls': '📊',
      'xml': '📄',
      'txt': '📝',
      'pdf': '📕'
    }

    return iconMap[ext] || '📄'
  }

  return {
    validateFile,
    formatFileSize,
    getFileIcon
  }
}
