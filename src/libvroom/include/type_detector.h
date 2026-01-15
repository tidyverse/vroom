/**
 * @file type_detector.h
 * @brief Backward compatibility header - includes libvroom_types.h
 *
 * @deprecated Use libvroom_types.h directly instead.
 *
 * This header is provided for backward compatibility only. The type detection
 * functionality has been moved to libvroom_types.h to make it clearly optional.
 * Type detection is an independent module that can be excluded from builds
 * via CMake option LIBVROOM_ENABLE_TYPE_DETECTION=OFF.
 */

#ifndef LIBVROOM_TYPE_DETECTOR_H
#define LIBVROOM_TYPE_DETECTOR_H

#include "libvroom_types.h"

#endif  // LIBVROOM_TYPE_DETECTOR_H
