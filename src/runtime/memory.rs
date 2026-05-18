pub fn runtime_rss_bytes() -> Option<usize> {
    platform_rss_bytes()
}

#[cfg(target_os = "linux")]
fn platform_rss_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<usize>().ok())?;
    kib.checked_mul(1024)
}

#[cfg(target_os = "macos")]
fn platform_rss_bytes() -> Option<usize> {
    use std::mem::{size_of, MaybeUninit};

    type KernReturn = i32;
    type MachMsgTypeNumber = u32;
    type Natural = u32;
    type TaskName = u32;

    const KERN_SUCCESS: KernReturn = 0;
    const MACH_TASK_BASIC_INFO: i32 = 20;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct TimeValue {
        seconds: i32,
        microseconds: i32,
    }

    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: TimeValue,
        system_time: TimeValue,
        policy: i32,
        suspend_count: i32,
    }

    extern "C" {
        fn mach_task_self() -> TaskName;
        fn task_info(
            target_task: TaskName,
            flavor: i32,
            task_info_out: *mut Natural,
            task_info_out_cnt: *mut MachMsgTypeNumber,
        ) -> KernReturn;
    }

    let mut info = MaybeUninit::<MachTaskBasicInfo>::uninit();
    let mut count = (size_of::<MachTaskBasicInfo>() / size_of::<Natural>()) as MachMsgTypeNumber;
    let result = unsafe {
        task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast::<Natural>(),
            &mut count,
        )
    };
    if result == KERN_SUCCESS {
        Some(unsafe { info.assume_init().resident_size as usize })
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_rss_bytes() -> Option<usize> {
    None
}
