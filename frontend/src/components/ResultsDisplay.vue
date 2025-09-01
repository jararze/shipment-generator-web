<script>
import { ref, reactive } from 'vue'
import { useShipmentApi, useFileHandling } from '@/composables/useApi'
import {
  CheckCircleIcon,
  ArrowDownTrayIcon,
  EyeIcon
} from '@heroicons/vue/24/outline'

export default {
  name: 'ResultsDisplay',
  components: {
    CheckCircleIcon,
    ArrowDownTrayIcon,
    EyeIcon
  },
  props: {
    job: {
      type: Object,
      default: null
    }
  },
  setup(props) {
    const { downloadFile, jobs } = useShipmentApi();
    const { getFileIcon } = useFileHandling()

    const isDownloading = reactive({})
    const isDownloadingAll = ref(false)

    const downloadAllFiles = async () => {
      if (!props.job?.result_files) return

      try {
        isDownloadingAll.value = true

        // Descargar archivos secuencialmente para evitar sobrecarga
        for (const filePath of props.job.result_files) {
          const newName = generateStandardName(filePath);
          await downloadFile(filePath, newName)
          // Pequeña pausa entre descargas
          await new Promise(resolve => setTimeout(resolve, 500))
        }
      } catch (error) {
        console.error('Error descargando todos los archivos:', error)
      } finally {
        isDownloadingAll.value = false
      }
    }

    const getFileName = (filePath) => {
      return filePath.split('/').pop() || filePath
    }

    const getFileType = (filePath) => {
      const fileName = getFileName(filePath)
      const ext = fileName.toLowerCase().split('.').pop()

      const typeMap = {
        'xml': 'Archivo XML de Envíos',
        'xlsx': 'Archivo Excel de Placas',
        'txt': 'Reporte de Validación',
        'log': 'Log de Procesamiento'
      }

      return typeMap[ext] || 'Archivo Generado'
    }

    const formatDate = (dateString) => {
      try {
        return new Date(dateString).toLocaleString('es-ES', {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit'
        })
      } catch {
        return dateString
      }
    }

    const getDuration = (startDate, endDate) => {
      try {
        const start = new Date(startDate)
        const end = new Date(endDate)
        const durationMs = end - start

        const seconds = Math.floor(durationMs / 1000)
        const minutes = Math.floor(seconds / 60)

        if (minutes > 0) {
          return `${minutes}m ${seconds % 60}s`
        } else {
          return `${seconds}s`
        }
      } catch {
        return 'N/A'
      }
    }

    const generateStandardName = (filePath) => {
      // Para depurar, podemos ver qué información tenemos al momento del clic
      console.log("Datos del trabajo al generar nombre:", props.job);

      // 1. Timestamp (siempre se puede generar)
      // 1. Timestamp (ahora prioriza la fecha del archivo)
      let timestamp;
      if (props.job?.file_date) {
        // Plan A: Usar la fecha que envía el backend (ej: "2025-08-01")
        // y la convertimos a "20250801"
        timestamp = props.job.file_date.replace(/-/g, '');
      } else {
        // Plan B (Fallback): Si no hay fecha del archivo, usar la fecha actual
        const now = new Date();
        timestamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`;
      }

      // 2. Tipo de archivo (con Plan B)
      let type = 'general';
      if (props.job?.file_type) {
        type = props.job.file_type;
      } else if (props.job?.filename) {
        const upperFilename = props.job.filename.toUpperCase();
        if (upperFilename.includes('BEER')) type = 'beer';
        else if (upperFilename.includes('SD')) type = 'sd';
        else if (upperFilename.includes('CB')) type = 'cb';
      }

      // 3. Descripción y Extensión (tomada siempre del 'filePath' original)
      const originalFilename = filePath.split('/').pop() || '';
      const extension = originalFilename.split('.').pop() || 'txt';
      let description = "rutas";
      if (extension === 'xlsx') {
        description = "placas";
      } else if (extension === 'txt') {
        description = "reporte";
      }

      // 4. Construir el nombre final
      const finalName = `${timestamp}_${type}_${description}.${extension}`;

      console.log("Nombre generado con fecha del archivo:", finalName);
      return finalName;
    };

    return {
      isDownloading,
      isDownloadingAll,
      generateStandardName,
      downloadFile,
      downloadAllFiles,
      getFileName,
      getFileType,
      getFileIcon,
      formatDate,
      getDuration
    }
  }
}
</script>

<template>
  <div v-if="job?.status === 'completed' && job.result_files?.length > 0"
       class="bg-green-50 border border-green-200 rounded-lg p-6 mb-6 fade-in">

    <!-- Header -->
    <div class="flex items-center space-x-3 mb-6">
      <CheckCircleIcon class="w-6 h-6 text-green-600" />
      <h2 class="text-xl font-semibold text-green-800">Archivos Generados</h2>
      <span class="bg-green-100 text-green-700 px-3 py-1 rounded-full text-sm font-medium">
        {{ job.result_files.length }} archivo{{ job.result_files.length !== 1 ? 's' : '' }}
      </span>
    </div>

    <!-- Lista de archivos -->
    <div class="space-y-3 mb-6">
      <div
        v-for="(filePath, index) in job.result_files"
        :key="index"
        class="bg-white rounded-lg border border-green-200 p-4 hover:shadow-md transition-shadow"
      >
        <div class="flex items-center justify-between">
          <div class="flex items-center space-x-4 flex-grow">
            <!-- Icono del archivo -->
            <div class="flex-shrink-0">
              <div class="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center">
                <span class="text-lg">{{ getFileIcon(filePath) }}</span>
              </div>
            </div>

            <!-- Información del archivo -->
            <div class="flex-grow">
              <p class="font-medium text-gray-900">{{ generateStandardName(filePath) }}</p>
              <p class="text-sm text-gray-600">{{ getFileType(filePath) }}</p>
            </div>

            <!-- Estado del archivo -->
            <div class="text-sm text-green-600 font-medium">
              ✓ Listo
            </div>
          </div>

          <!-- Botones de acción -->
          <div class="flex items-center space-x-2 flex-shrink-0">
            <button
              @click="downloadFile(filePath, generateStandardName(filePath))"
              :disabled="isDownloading[filePath]"
              class="flex items-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-lg
                     hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed
                     transition-colors font-medium"
              :title="isDownloading[filePath] ? 'Descargando...' : 'Descargar archivo'"
            >
              <ArrowDownTrayIcon v-if="!isDownloading[filePath]" class="w-4 h-4" />
              <div v-else class="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
              <span class="hidden sm:inline">
                {{ isDownloading[filePath] ? 'Descargando...' : 'Descargar' }}
              </span>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Botón para descargar todos -->
    <div v-if="job.result_files.length > 1" class="text-center">
      <button
        @click="downloadAllFiles"
        :disabled="isDownloadingAll"
        class="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700
               disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors
               font-medium flex items-center space-x-2 mx-auto"
      >
        <ArrowDownTrayIcon v-if="!isDownloadingAll" class="w-5 h-5" />
        <div v-else class="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
        <span>{{ isDownloadingAll ? 'Descargando todos...' : 'Descargar Todos los Archivos' }}</span>
      </button>
    </div>

    <!-- Resumen de procesamiento -->
    <div v-if="job.completed_at" class="mt-6 pt-4 border-t border-green-200">
      <div class="grid md:grid-cols-3 gap-4 text-sm">
        <div>
          <span class="font-medium text-green-700">Completado:</span>
          <p class="text-green-600">{{ formatDate(job.completed_at) }}</p>
        </div>
        <div v-if="job.started_at">
          <span class="font-medium text-green-700">Duración:</span>
          <p class="text-green-600">{{ getDuration(job.started_at, job.completed_at) }}</p>
        </div>
        <div>
          <span class="font-medium text-green-700">Estado:</span>
          <p class="text-green-600">✓ Procesamiento exitoso</p>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>

</style>
