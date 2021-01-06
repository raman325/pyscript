"""Component to allow running Python scripts."""

import asyncio
from datetime import timedelta
import glob
import json
import logging
import os
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Union

import voluptuous as vol
from watchdog.events import DirModifiedEvent, FileSystemEvent, FileSystemEventHandler
import watchdog.observers

from homeassistant.components.automation import DOMAIN as AUTO_DOMAIN
from homeassistant.config import async_hass_config_yaml
from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import (
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
    SERVICE_RELOAD,
    STATE_OFF,
    STATE_ON,
)
from homeassistant.core import Config, HomeAssistant, ServiceCall, split_entity_id
from homeassistant.exceptions import HomeAssistantError
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.dispatcher import async_dispatcher_connect, async_dispatcher_send
from homeassistant.helpers.entity import ToggleEntity
from homeassistant.helpers.entity_platform import EntityPlatform
from homeassistant.helpers.event import Event as HAEvent
from homeassistant.helpers.restore_state import RestoreEntity, RestoreStateData
from homeassistant.loader import bind_hass

from .const import (
    CONF_ALLOW_ALL_IMPORTS,
    CONF_HASS_IS_GLOBAL,
    CONFIG_ENTRY,
    CONFIG_ENTRY_OLD,
    DOMAIN,
    FOLDER,
    LOGGER_PATH,
    REQUIREMENTS_FILE,
    SERVICE_JUPYTER_KERNEL_START,
    UNSUB_LISTENERS,
    WATCHDOG_OBSERVER,
    WATCHDOG_TASK,
)
from .eval import AstEval, EvalFunc
from .event import Event
from .function import Function
from .global_ctx import GlobalContext, GlobalContextMgr
from .jupyter_kernel import Kernel
from .mqtt import Mqtt
from .requirements import install_requirements
from .state import State, StateVal
from .trigger import TrigTime

_LOGGER = logging.getLogger(LOGGER_PATH)

PYSCRIPT_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_ALLOW_ALL_IMPORTS, default=False): cv.boolean,
        vol.Optional(CONF_HASS_IS_GLOBAL, default=False): cv.boolean,
    },
    extra=vol.ALLOW_EXTRA,
)

CONFIG_SCHEMA = vol.Schema({DOMAIN: PYSCRIPT_SCHEMA}, extra=vol.ALLOW_EXTRA)


async def async_setup(hass: HomeAssistant, config: Config) -> bool:
    """Component setup, run import config flow for each entry in config."""
    await restore_state(hass)
    if DOMAIN in config:
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_IMPORT}, data=config[DOMAIN]
            )
        )

    return True


async def restore_state(hass: HomeAssistant) -> None:
    """Restores the persisted pyscript state."""
    restore_data = await RestoreStateData.async_get_instance(hass)
    for entity_id, value in restore_data.last_states.items():
        if entity_id.startswith("pyscript."):
            last_state = value.state
            hass.states.async_set(entity_id, last_state.state, last_state.attributes)


async def update_yaml_config(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Update the yaml config."""
    try:
        conf = await async_hass_config_yaml(hass)
    except HomeAssistantError as err:
        _LOGGER.error(err)
        return

    config = PYSCRIPT_SCHEMA(conf.get(DOMAIN, {}))

    #
    # If data in config doesn't match config entry, trigger a config import
    # so that the config entry can get updated
    #
    if config != config_entry.data:
        await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_IMPORT}, data=config)

    #
    # if hass_is_global or allow_all_imports have changed, we need to reload all scripts
    # since they affect all scripts
    #
    config_save = {
        param: config_entry.data.get(param, False) for param in {CONF_HASS_IS_GLOBAL, CONF_ALLOW_ALL_IMPORTS}
    }
    if DOMAIN not in hass.data:
        hass.data.setdefault(DOMAIN, {})
    if CONFIG_ENTRY_OLD in hass.data[DOMAIN]:
        old_entry = hass.data[DOMAIN][CONFIG_ENTRY_OLD]
        hass.data[DOMAIN][CONFIG_ENTRY_OLD] = config_save
        for param in {CONF_HASS_IS_GLOBAL, CONF_ALLOW_ALL_IMPORTS}:
            if old_entry.get(param, False) != config_entry.data.get(param, False):
                return True
    hass.data[DOMAIN][CONFIG_ENTRY_OLD] = config_save
    return False


def start_global_contexts(global_ctx_only: str = None) -> None:
    """Start all the file and apps global contexts."""
    start_list = []
    for global_ctx_name, global_ctx in GlobalContextMgr.items():
        idx = global_ctx_name.find(".")
        if idx < 0 or global_ctx_name[0:idx] not in {"file", "apps", "scripts"}:
            continue
        if global_ctx_only is not None:
            if global_ctx_name != global_ctx_only and not global_ctx_name.startswith(global_ctx_only + "."):
                continue
        global_ctx.set_auto_start(True)
        start_list.append(global_ctx)
    for global_ctx in start_list:
        global_ctx.start()


async def watchdog_start(
    hass: HomeAssistant, pyscript_folder: str, reload_scripts_handler: Callable[[None], None]
) -> None:
    """Start watchdog thread to look for changed files in pyscript_folder."""
    if WATCHDOG_OBSERVER in hass.data[DOMAIN]:
        return

    class WatchDogHandler(FileSystemEventHandler):
        """Class for handling watchdog events."""

        def __init__(self, watchdog_q: asyncio.Queue) -> None:
            self.watchdog_q = watchdog_q

        def process(self, event: FileSystemEvent) -> None:
            """Send watchdog events to main loop task."""
            _LOGGER.debug("watchdog process(%s)", event)
            hass.loop.call_soon_threadsafe(self.watchdog_q.put_nowait, event)

        def on_modified(self, event: FileSystemEvent) -> None:
            """File modified."""
            self.process(event)

        def on_moved(self, event: FileSystemEvent) -> None:
            """File moved."""
            self.process(event)

        def on_created(self, event: FileSystemEvent) -> None:
            """File created."""
            self.process(event)

        def on_deleted(self, event: FileSystemEvent) -> None:
            """File deleted."""
            self.process(event)

    async def task_watchdog(watchdog_q: asyncio.Queue) -> None:
        def check_event(event, do_reload: bool) -> bool:
            """Check if event should trigger a reload."""
            if event.is_directory:
                # don't reload if it's just a directory modified
                if isinstance(event, DirModifiedEvent):
                    return do_reload
                return True
            # only reload if it's a script, yaml, or requirements.txt file
            for valid_suffix in [".py", ".yaml", "/" + REQUIREMENTS_FILE]:
                if event.src_path.endswith(valid_suffix):
                    return True
            return do_reload

        while True:
            try:
                #
                # since some file/dir changes create multiple events, we consume all
                # events in a small window; first # wait indefinitely for next event
                #
                do_reload = check_event(await watchdog_q.get(), False)
                #
                # now consume all additional events with 50ms timeout or 500ms elapsed
                #
                t_start = time.monotonic()
                while time.monotonic() - t_start < 0.5:
                    try:
                        do_reload = check_event(
                            await asyncio.wait_for(watchdog_q.get(), timeout=0.05), do_reload
                        )
                    except asyncio.TimeoutError:
                        break
                if do_reload:
                    await reload_scripts_handler(None)

            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.error("task_watchdog: got exception %s", traceback.format_exc(-1))

    watchdog_q = asyncio.Queue(0)
    observer = watchdog.observers.Observer()
    if observer is not None:
        # don't run watchdog when we are testing (Observer() patches to None)
        hass.data[DOMAIN][WATCHDOG_OBSERVER] = observer
        hass.data[DOMAIN][WATCHDOG_TASK] = Function.create_task(task_watchdog(watchdog_q))

        observer.schedule(WatchDogHandler(watchdog_q), pyscript_folder, recursive=True)


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Initialize the pyscript config entry."""
    global_ctx_only = None
    if Function.hass:
        #
        # reload yaml if this isn't the first time (ie, on reload)
        #
        if await update_yaml_config(hass, config_entry):
            global_ctx_only = "*"

    Function.init(hass)
    Event.init(hass)
    Mqtt.init(hass)
    TrigTime.init(hass)
    State.init(hass)
    State.register_functions()
    GlobalContextMgr.init()

    pyscript_folder = hass.config.path(FOLDER)
    if not await hass.async_add_executor_job(os.path.isdir, pyscript_folder):
        _LOGGER.debug("Folder %s not found in configuration folder, creating it", FOLDER)
        await hass.async_add_executor_job(os.makedirs, pyscript_folder)

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][CONFIG_ENTRY] = config_entry
    hass.data[DOMAIN][UNSUB_LISTENERS] = []
    auto_entity_platform = EntityPlatform(
        hass=hass,
        logger=_LOGGER,
        domain=AUTO_DOMAIN,
        platform_name=DOMAIN,
        platform=None,
        scan_interval=timedelta(seconds=0),
        entity_namespace=DOMAIN,
    )
    auto_entity_platform.config_entry = config_entry
    hass.data[DOMAIN][AUTO_DOMAIN] = auto_entity_platform

    State.set_pyscript_config(config_entry.data)

    await install_requirements(hass, config_entry, pyscript_folder)
    await load_scripts(hass, config_entry.data, global_ctx_only=global_ctx_only)

    async def reload_scripts_handler(call: ServiceCall) -> None:
        """Handle reload service calls."""
        _LOGGER.debug("reload: yaml, reloading scripts, and restarting")

        global_ctx_only = call.data.get("global_ctx", None) if call else None

        if await update_yaml_config(hass, config_entry):
            global_ctx_only = "*"
        State.set_pyscript_config(config_entry.data)

        await State.get_service_params()

        await install_requirements(hass, config_entry, pyscript_folder)
        await load_scripts(hass, config_entry.data, global_ctx_only=global_ctx_only)

        start_global_contexts(global_ctx_only=global_ctx_only)

    hass.services.async_register(DOMAIN, SERVICE_RELOAD, reload_scripts_handler)

    async def jupyter_kernel_start(call: ServiceCall) -> None:
        """Handle Jupyter kernel start call."""
        _LOGGER.debug("service call to jupyter_kernel_start: %s", call.data)

        global_ctx_name = GlobalContextMgr.new_name("jupyter_")
        global_ctx = GlobalContext(
            global_ctx_name, global_sym_table={"__name__": global_ctx_name}, manager=GlobalContextMgr
        )
        global_ctx.set_auto_start(True)
        GlobalContextMgr.set(global_ctx_name, global_ctx)

        ast_ctx = AstEval(global_ctx_name, global_ctx)
        Function.install_ast_funcs(ast_ctx)
        kernel = Kernel(call.data, ast_ctx, global_ctx, global_ctx_name)
        await kernel.session_start()
        hass.states.async_set(call.data["state_var"], json.dumps(kernel.get_ports()))

        def state_var_remove():
            hass.states.async_remove(call.data["state_var"])

        kernel.set_session_cleanup_callback(state_var_remove)

    hass.services.async_register(DOMAIN, SERVICE_JUPYTER_KERNEL_START, jupyter_kernel_start)

    async def state_changed(event: HAEvent) -> None:
        var_name = event.data["entity_id"]
        if event.data.get("new_state", None):
            new_val = StateVal(event.data["new_state"])
        else:
            # state variable has been deleted
            new_val = None

        if event.data.get("old_state", None):
            old_val = StateVal(event.data["old_state"])
        else:
            # no previous state
            old_val = None

        new_vars = {var_name: new_val, f"{var_name}.old": old_val}
        func_args = {
            "trigger_type": "state",
            "var_name": var_name,
            "value": new_val,
            "old_value": old_val,
            "context": event.context,
        }
        await State.update(new_vars, func_args)

    async def hass_started(event: HAEvent) -> None:
        _LOGGER.debug("adding state changed listener and starting global contexts")
        await State.get_service_params()
        hass.data[DOMAIN][UNSUB_LISTENERS].append(hass.bus.async_listen(EVENT_STATE_CHANGED, state_changed))
        start_global_contexts()
        if WATCHDOG_OBSERVER in hass.data[DOMAIN]:
            observer = hass.data[DOMAIN][WATCHDOG_OBSERVER]
            observer.start()

    async def hass_stop(event: HAEvent) -> None:
        if WATCHDOG_OBSERVER in hass.data[DOMAIN]:
            observer = hass.data[DOMAIN][WATCHDOG_OBSERVER]
            observer.stop()
            observer.join()
            del hass.data[DOMAIN][WATCHDOG_OBSERVER]
            Function.reaper_cancel(hass.data[DOMAIN][WATCHDOG_TASK])
            del hass.data[DOMAIN][WATCHDOG_TASK]

        _LOGGER.debug("stopping global contexts")
        await unload_scripts(unload_all=True)
        # sync with waiter, and then tell waiter and reaper tasks to exit
        await Function.waiter_sync()
        await Function.waiter_stop()
        await Function.reaper_stop()

    # Store callbacks to event listeners so we can unsubscribe on unload
    hass.data[DOMAIN][UNSUB_LISTENERS].append(
        hass.bus.async_listen(EVENT_HOMEASSISTANT_STARTED, hass_started)
    )
    hass.data[DOMAIN][UNSUB_LISTENERS].append(hass.bus.async_listen(EVENT_HOMEASSISTANT_STOP, hass_stop))

    await watchdog_start(hass, pyscript_folder, reload_scripts_handler)

    return True


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading all scripts")
    await unload_scripts()

    for unsub_listener in hass.data[DOMAIN][UNSUB_LISTENERS]:
        unsub_listener()
    hass.data[DOMAIN][UNSUB_LISTENERS] = []

    return True


async def unload_scripts(global_ctx_only: str = None, unload_all: bool = False) -> None:
    """Unload all scripts from GlobalContextMgr with given name prefixes."""
    ctx_delete = {}
    for global_ctx_name, global_ctx in GlobalContextMgr.items():
        if not unload_all:
            idx = global_ctx_name.find(".")
            if idx < 0 or global_ctx_name[0:idx] not in {"file", "apps", "modules", "scripts"}:
                continue
        if global_ctx_only is not None:
            if global_ctx_name != global_ctx_only and not global_ctx_name.startswith(global_ctx_only + "."):
                continue
        global_ctx.stop()
        ctx_delete[global_ctx_name] = global_ctx
    for global_ctx_name, global_ctx in ctx_delete.items():
        GlobalContextMgr.delete(global_ctx_name)
    await Function.waiter_sync()


@bind_hass
async def load_scripts(hass: HomeAssistant, config_data: Dict[str, Any], global_ctx_only: str = None):
    """Load all python scripts in FOLDER."""

    class SourceFile:
        """Class for information about a source file."""

        def __init__(
            self,
            global_ctx_name=None,
            file_path=None,
            rel_path=None,
            rel_import_path=None,
            fq_mod_name=None,
            check_config=None,
            app_config=None,
            source=None,
            mtime=None,
            autoload=None,
        ):
            self.global_ctx_name = global_ctx_name
            self.file_path = file_path
            self.rel_path = rel_path
            self.rel_import_path = rel_import_path
            self.fq_mod_name = fq_mod_name
            self.check_config = check_config
            self.app_config = app_config
            self.source = source
            self.mtime = mtime
            self.autoload = autoload
            self.force = False

    pyscript_dir = hass.config.path(FOLDER)

    def glob_read_files(
        load_paths: List[Set[Union[str, bool]]], apps_config: Dict[str, Any]
    ) -> Dict[str, SourceFile]:
        """Expand globs and read all the source files."""
        ctx2source = {}
        for path, match, check_config, autoload in load_paths:
            for this_path in sorted(glob.glob(os.path.join(pyscript_dir, path, match), recursive=True)):
                rel_import_path = None
                rel_path = this_path
                if rel_path.startswith(pyscript_dir):
                    rel_path = rel_path[len(pyscript_dir) :]
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]
                if rel_path[0] == "#" or rel_path.find("/#") >= 0:
                    # skip "commented" files and directories
                    continue
                mod_name = rel_path[0:-3]
                if mod_name.endswith("/__init__"):
                    rel_import_path = mod_name
                    mod_name = mod_name[: -len("/__init__")]
                mod_name = mod_name.replace("/", ".")
                if path == "":
                    global_ctx_name = f"file.{mod_name}"
                    fq_mod_name = mod_name
                else:
                    fq_mod_name = global_ctx_name = mod_name
                    i = fq_mod_name.find(".")
                    if i >= 0:
                        fq_mod_name = fq_mod_name[i + 1 :]
                app_config = None

                if global_ctx_name in ctx2source:
                    # the globs result in apps/APP/__init__.py matching twice, so skip the 2nd time
                    # also skip apps/APP.py if apps/APP/__init__.py is present
                    continue

                if check_config:
                    app_name = fq_mod_name
                    i = app_name.find(".")
                    if i >= 0:
                        app_name = app_name[0:i]
                    if not isinstance(apps_config, dict) or app_name not in apps_config:
                        _LOGGER.debug(
                            "load_scripts: skipping %s (app_name=%s) because config not present",
                            this_path,
                            app_name,
                        )
                        continue
                    app_config = apps_config[app_name]

                try:
                    with open(this_path) as file_desc:
                        source = file_desc.read()
                    mtime = os.path.getmtime(this_path)
                except Exception as exc:
                    _LOGGER.error("load_scripts: skipping %s due to exception %s", this_path, exc)
                    continue

                ctx2source[global_ctx_name] = SourceFile(
                    global_ctx_name=global_ctx_name,
                    file_path=this_path,
                    rel_path=rel_path,
                    rel_import_path=rel_import_path,
                    fq_mod_name=fq_mod_name,
                    check_config=check_config,
                    app_config=app_config,
                    source=source,
                    mtime=mtime,
                    autoload=autoload,
                )

        return ctx2source

    load_paths = [
        # path, glob, check_config, autoload
        ["", "*.py", False, True],
        ["apps", "*/__init__.py", True, True],
        ["apps", "*.py", True, True],
        ["apps", "*/**/*.py", False, False],
        ["modules", "*/__init__.py", False, False],
        ["modules", "*.py", False, False],
        ["modules", "*/**/*.py", False, False],
        ["scripts", "**/*.py", False, True],
    ]

    #
    # get current global contexts
    #
    ctx_all = {}
    for global_ctx_name, global_ctx in GlobalContextMgr.items():
        idx = global_ctx_name.find(".")
        if idx < 0 or global_ctx_name[0:idx] not in {"file", "apps", "modules", "scripts"}:
            continue
        ctx_all[global_ctx_name] = global_ctx

    #
    # get list and contents of all source files
    #
    apps_config = config_data.get("apps", None)
    ctx2files = await hass.async_add_executor_job(glob_read_files, load_paths, apps_config)

    #
    # figure out what to reload based on global_ctx_only and what's changed
    #
    ctx_delete = set()
    if global_ctx_only is not None and global_ctx_only != "*":
        if global_ctx_only not in ctx_all and global_ctx_only not in ctx2files:
            _LOGGER.error("pyscript.reload: no global context '%s' to reload", global_ctx_only)
            return
        if global_ctx_only not in ctx2files:
            ctx_delete.add(global_ctx_only)
        else:
            ctx2files[global_ctx_only].force = True
    elif global_ctx_only == "*":
        ctx_delete = set(ctx_all.keys())
        for _, src_info in ctx2files.items():
            src_info.force = True
    else:
        # delete all global_ctxs that aren't present in current files
        for global_ctx_name, global_ctx in ctx_all.items():
            if global_ctx_name not in ctx2files:
                ctx_delete.add(global_ctx_name)
        # delete all global_ctxs that have changeed source or mtime
        for global_ctx_name, src_info in ctx2files.items():
            if global_ctx_name in ctx_all:
                ctx = ctx_all[global_ctx_name]
                if (
                    src_info.source != ctx.get_source()
                    or src_info.app_config != ctx.get_app_config()
                    or src_info.mtime != ctx.get_mtime()
                ):
                    ctx_delete.add(global_ctx_name)
                    src_info.force = True
            else:
                src_info.force = src_info.autoload

    #
    # force reload if any files uses a module that is bring reloaded by
    # recursively following each import; first find which modules are
    # being reloaded
    #
    will_reload = set()
    for global_ctx_name, src_info in ctx2files.items():
        if global_ctx_name.startswith("modules.") and (global_ctx_name in ctx_delete or src_info.force):
            parts = global_ctx_name.split(".")
            root = f"{parts[0]}.{parts[1]}"
            will_reload.add(root)

    if len(will_reload) > 0:

        def import_recurse(ctx_name, visited, ctx2imports):
            if ctx_name in visited or ctx_name in ctx2imports:
                return ctx2imports.get(ctx_name, set())
            visited.add(ctx_name)
            ctx = GlobalContextMgr.get(ctx_name)
            if not ctx:
                return set()
            ctx2imports[ctx_name] = set()
            for imp_name in ctx.get_imports():
                ctx2imports[ctx_name].add(imp_name)
                ctx2imports[ctx_name].update(import_recurse(imp_name, visited, ctx2imports))
            return ctx2imports[ctx_name]

        ctx2imports = {}
        for global_ctx_name, global_ctx in ctx_all.items():
            if global_ctx_name not in ctx2imports:
                visited = set()
                import_recurse(global_ctx_name, visited, ctx2imports)
            for mod_name in ctx2imports.get(global_ctx_name, set()):
                parts = mod_name.split(".")
                root = f"{parts[0]}.{parts[1]}"
                if root in will_reload:
                    ctx_delete.add(global_ctx_name)
                    if global_ctx_name in ctx2files:
                        ctx2files[global_ctx_name].force = True

    #
    # if any file in an app or module has changed, then reload just the top-level
    # __init__.py or module/app .py file, and delete everything else
    #
    done = set()
    for global_ctx_name, src_info in ctx2files.items():
        if not src_info.force:
            continue
        if not global_ctx_name.startswith("apps.") and not global_ctx_name.startswith("modules."):
            continue
        parts = global_ctx_name.split(".")
        root = f"{parts[0]}.{parts[1]}"
        if root in done:
            continue
        pkg_path = f"{parts[0]}/{parts[1]}/__init__.py"
        mod_path = f"{parts[0]}/{parts[1]}.py"
        for ctx_name, this_src_info in ctx2files.items():
            if ctx_name == root or ctx_name.startswith(f"{root}."):
                if this_src_info.rel_path in {pkg_path, mod_path}:
                    this_src_info.force = True
                else:
                    this_src_info.force = False
                ctx_delete.add(ctx_name)
        done.add(root)

    # Get automation entity_platform and entity_registry so we can add and remove entities
    auto_platform: EntityPlatform = hass.data[DOMAIN][AUTO_DOMAIN]
    entity_registry = await hass.helpers.entity_registry.async_get_registry()

    #
    # delete contexts that are no longer needed or will be reloaded, and delete corresponding
    # entities
    #
    for global_ctx_name in ctx_delete:
        if global_ctx_name in ctx_all:
            global_ctx = ctx_all[global_ctx_name]

            entity_id = entity_registry.async_get_entity_id(
                AUTO_DOMAIN, DOMAIN, f"{DOMAIN}.{global_ctx.get_name()}"
            )
            if entity_id is not None:
                await auto_platform.async_remove_entity(entity_id)
                entity_registry.async_remove(entity_id)

            for trigger in global_ctx.triggers:
                entity_id = entity_registry.async_get_entity_id(
                    AUTO_DOMAIN,
                    DOMAIN,
                    (
                        f"{DOMAIN}.{global_ctx.get_name()}.{trigger.get_name()}."
                        f"{json.dumps(trigger.trigger_decorators())}"
                    ),
                )
                if entity_id is not None:
                    await auto_platform.async_remove_entity(entity_id)
                    entity_registry.async_remove(entity_id)

            global_ctx.stop()
            if global_ctx_name not in ctx2files or not ctx2files[global_ctx_name].autoload:
                _LOGGER.info("Unloaded %s", global_ctx.get_file_path())
            GlobalContextMgr.delete(global_ctx_name)
    await Function.waiter_sync()

    #
    # now load the requested files, and files that depend on loaded files
    #
    ctx_to_add = []
    for global_ctx_name, src_info in sorted(ctx2files.items()):
        if not src_info.autoload or not src_info.force:
            continue
        global_ctx = GlobalContext(
            src_info.global_ctx_name,
            global_sym_table={"__name__": src_info.fq_mod_name},
            manager=GlobalContextMgr,
            rel_import_path=src_info.rel_import_path,
            app_config=src_info.app_config,
            source=src_info.source,
            mtime=src_info.mtime,
        )
        reload = src_info.global_ctx_name in ctx_delete
        load_file_args = {
            "global_ctx": global_ctx,
            "file_path": src_info.file_path,
            "source": src_info.source,
            "reload": reload,
        }
        await GlobalContextMgr.load_file(**load_file_args)
        ctx_to_add.append(load_file_args)

    #
    # now create entities for every automation, and every file/app/module
    # that has more than one automation
    #
    if ctx_to_add:
        auto_entities_to_add = []
        for load_file_args in ctx_to_add:
            file_entity = None

            # Filter EvalFunc down to non service based triggers
            automation_triggers = [
                trigger for trigger in load_file_args["global_ctx"].triggers if not trigger.trigger_service
            ]

            # We only need a file/module/app entity if we are registering more than one automation for it
            if len(automation_triggers) > 1:
                file_entity = PyscriptFileEntity(load_file_args)
                auto_entities_to_add.append(file_entity)

            child_entities = [
                PyscriptChildEntity(load_file_args, trigger, file_entity) for trigger in automation_triggers
            ]
            if file_entity:
                file_entity.link_to_child_entities(child_entities)
            auto_entities_to_add.extend(child_entities)

        await auto_platform.async_add_entities(auto_entities_to_add)


class PyscriptFileEntity(ToggleEntity, RestoreEntity):
    """Entity to show state of and control a pyscript file."""

    def __init__(self, load_file_args: Dict[str, Union[str, bool, GlobalContext]]) -> None:
        """Initialize a pyscript file entity."""
        self._ctx: GlobalContext = load_file_args["global_ctx"]
        self._name: str = self._ctx.get_name()
        self._file_path: str = load_file_args["file_path"]
        self._source: str = load_file_args["source"]
        self._state: str = STATE_ON
        self._id: str = f"{DOMAIN}.{self._name}"
        self._logger = None
        self._child_entities = []

    @property
    def name(self):
        """Name of the automation."""
        return self._name

    @property
    def unique_id(self):
        """Return unique ID."""
        return self._id

    @property
    def should_poll(self):
        """No polling needed for automation entities."""
        return False

    async def link_to_child_entities(self, child_entities) -> None:
        """Link entity to child entities."""
        if self._child_entities:
            pass
        self._child_entities = child_entities

    @property
    def is_on(self) -> bool:
        """Return True if entity is on."""
        return self._state == STATE_ON

    async def async_added_to_hass(self) -> None:
        """Startup with initial state or previous state."""
        self._logger = logging.getLogger(f"{__name__}.{split_entity_id(self.entity_id)[1]}")

        state = await self.async_get_last_state()
        if state:
            enable_automation = state.state == STATE_ON
            self._logger.debug(
                "Loaded automation %s with state %s from state " " storage last state %s",
                self.entity_id,
                enable_automation,
                state,
            )
        else:
            enable_automation = STATE_ON
            self._logger.debug(
                "Automation %s not in state storage, state %s from default is used",
                self.entity_id,
                enable_automation,
            )

        if not enable_automation:
            await self.async_turn_off()

    async def async_reload_file(self) -> None:
        """Reload file into GlobalContextMgr."""
        await GlobalContextMgr.load_file(self._ctx, self._file_path, self._source, True)

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the entity on and update the state."""
        await self.async_reload_file()
        self._state = STATE_ON
        self.async_write_ha_state()
        async_dispatcher_send(self.hass, f"{DOMAIN}.{self.unique_id}", self.state)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the entity off."""
        self._ctx.stop()
        GlobalContextMgr.delete(self._ctx)
        self._state = STATE_OFF
        self.async_write_ha_state()
        async_dispatcher_send(self.hass, f"{DOMAIN}.{self.unique_id}", self.state)

    async def async_trigger(self, run_variables, context=None, skip_condition=False):
        """Trigger automation.

        This method is a coroutine.
        """
        raise NotImplementedError

    @property
    def device_state_attributes(self):
        """Return automation attributes."""
        if self._id is None:
            return None

        return {"id": self._id}


class PyscriptChildEntity(ToggleEntity, RestoreEntity):
    """Entity to show state of and control a pyscript automation."""

    def __init__(
        self,
        load_file_args: Dict[str, Union[str, bool, GlobalContext]],
        trigger: EvalFunc,
        parent_entity: Optional[PyscriptFileEntity],
    ) -> None:
        """Initialize pyscript automation entity."""
        self._ctx: GlobalContext = load_file_args["global_ctx"]
        self._trigger: EvalFunc = trigger
        self._parent_entity = parent_entity
        self._name: str = f"{self._ctx.get_name()} - {AUTO_DOMAIN}.{self._trigger.get_name()}"
        self._file_path: str = load_file_args["file_path"]
        self._source: str = load_file_args["source"]
        self._state: str = STATE_ON
        self._decorators = self._trigger.trigger_decorators()
        self._id: str = (
            f"{DOMAIN}.{self._ctx.get_name()}.{self._trigger.get_name()}.{json.dumps(self._decorators)}"
        )
        self._logger = None

    @property
    def name(self):
        """Name of the automation."""
        return self._name

    @property
    def unique_id(self):
        """Return unique ID."""
        return self._id

    @property
    def should_poll(self):
        """No polling needed for automation entities."""
        return False

    @property
    def is_on(self) -> bool:
        """Return True if entity is on."""
        return self._state == STATE_ON and (
            self._parent_entity is None or self._parent_entity.state == STATE_ON
        )

    async def async_added_to_hass(self) -> None:
        """Startup with initial state or previous state."""
        self._logger = logging.getLogger(f"{__name__}.{split_entity_id(self.entity_id)[1]}")

        state = await self.async_get_last_state()
        if state:
            enable_automation = state.state == STATE_ON
            self._logger.debug(
                "Loaded automation %s with state %s from state " " storage last state %s",
                self.entity_id,
                enable_automation,
                state,
            )
        else:
            enable_automation = STATE_ON
            self._logger.debug(
                "Automation %s not in state storage, state %s from default is used",
                self.entity_id,
                enable_automation,
            )

        if not enable_automation:
            await self.async_turn_off()

        async def handle_parent_state_change(state: str) -> None:
            if state == STATE_ON and self._state == STATE_OFF:
                await self.async_turn_off()
            elif state == STATE_OFF:
                self.async_write_ha_state()

        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, f"{DOMAIN}.{self._parent_entity.unique_id}", handle_parent_state_change
            )
        )

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the entity on and update the state."""
        self._state = STATE_ON
        await self._parent_entity.async_turn_on(**kwargs)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the entity off."""
        self._trigger.trigger_stop()
        self._state = STATE_OFF
        self.async_write_ha_state()

    async def async_trigger(self, run_variables, context=None, skip_condition=False):
        """Trigger automation.

        This method is a coroutine.
        """
        raise NotImplementedError

    @property
    def device_state_attributes(self):
        """Return automation attributes."""
        data = {}
        if self._id is not None:
            data["id"] = self._id

        if self._decorators is not None:
            data["decorators"] = self._decorators

        if self.state == STATE_OFF:
            data["disabled_by_parent"] = self._parent_entity and self._parent_entity.state == STATE_OFF
            data["disabled_by_self"] = self._state == STATE_OFF

        return data
