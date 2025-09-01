<script>
import { ref, computed } from 'vue'
import { useFileHandling } from '@/composables/useApi'
import { ArrowUpTrayIcon, CheckCircleIcon, XMarkIcon, ExclamationTriangleIcon } from '@heroicons/vue/24/outline'

export default {
  name: 'FileUpload',
  components: {
    ArrowUpTrayIcon,
    CheckCircleIcon,
    XMarkIcon,
    ExclamationTriangleIcon,
  },
  emits: ['file-selected', 'file-cleared'],
  setup(props, { emit }) {
    const selectedFile = ref(null)
    const isDragging = ref(false)
    const validationErrors = ref([])

    const { validateFile, formatFileSize } = useFileHandling()

    const handleFileSelect = (event) => {
      const file = event.target.files[0]
      processFile(file)
    }

    const handleDrop = (event) => {
      event.preventDefault()
      isDragging.value = false

      const files = event.dataTransfer.files
      if (files.length > 0) {
        processFile(files[0])
      }
    }

    const processFile = (file) => {
      if (!file) return

      const validation = validateFile(file)

      if (validation.isValid) {
        selectedFile.value = file
        validationErrors.value = []
        emit('file-selected', file)
      } else {
        validationErrors.value = validation.errors
        selectedFile.value = null
        emit('file-cleared')
      }
    }

    const clearFile = () => {
      selectedFile.value = null
      validationErrors.value = []
      emit('file-cleared')

      // Limpiar input
      const fileInput = document.querySelector('input[type="file"]')
      if (fileInput) fileInput.value = ''
    }

    return {
      selectedFile,
      isDragging,
      validationErrors,
      handleFileSelect,
      handleDrop,
      clearFile,
      formatFileSize,
    }
  },
}
</script>

<template>
  <div class="bg-white rounded-lg shadow-md p-6 mb-6">
    <h2 class="text-xl font-semibold mb-4 flex items-center">
      <ArrowUpTrayIcon class="w-5 h-5 mr-2" />
      Subir Archivo Excel
    </h2>

    <div
      @drop="handleDrop"
      @dragover.prevent
      @dragenter.prevent
      @dragleave="isDragging = false"
      class="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center transition-all duration-200"
      :class="{ 'drag-over': isDragging }"
    >
      <ArrowUpTrayIcon class="w-16 h-16 mx-auto text-gray-400 mb-4" />

      <div v-if="!selectedFile">
        <p class="text-gray-600 mb-4 text-lg">
          Arrastra tu archivo Excel aquí o haz clic para seleccionar
        </p>
        <p class="text-sm text-gray-500 mb-6">
          Formatos soportados: .xlsx, .xlsm, .xls (máximo 50MB)
        </p>

        <input
          ref="fileInput"
          type="file"
          accept=".xlsx,.xlsm,.xls"
          @change="handleFileSelect"
          class="hidden"
        />

        <button
          @click="$refs.fileInput.click()"
          class="bg-blue-600 text-white px-8 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium text-lg"
        >
          Seleccionar Archivo
        </button>
      </div>

      <div v-else class="bg-green-50 border border-green-200 rounded-lg p-6">
        <div class="flex items-center justify-center space-x-4">
          <div class="flex-shrink-0">
            <CheckCircleIcon class="w-8 h-8 text-green-600" />
          </div>
          <div class="flex-grow text-left">
            <p class="font-medium text-green-800 text-lg">{{ selectedFile.name }}</p>
            <p class="text-sm text-green-600">{{ formatFileSize(selectedFile.size) }}</p>
            <p class="text-xs text-green-500 mt-1">Archivo válido • Listo para procesar</p>
          </div>
          <button
            @click="clearFile"
            class="flex-shrink-0 text-red-500 hover:text-red-700 p-2 rounded-full hover:bg-red-50 transition-colors"
            title="Eliminar archivo"
          >
            <XMarkIcon class="w-6 h-6" />
          </button>
        </div>
      </div>
    </div>

    <!-- Errores de validación -->
    <div v-if="validationErrors.length > 0" class="mt-4">
      <div class="bg-red-50 border border-red-200 rounded-lg p-4">
        <div class="flex items-start space-x-3">
          <ExclamationTriangleIcon class="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" />
          <div>
            <h4 class="font-medium text-red-800">Errores de validación:</h4>
            <ul class="mt-2 text-sm text-red-700 space-y-1">
              <li v-for="error in validationErrors" :key="error">• {{ error }}</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped></style>
