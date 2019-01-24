import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {notify} from 'src/shared/actions/notifications'
import {presentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'

import {
    ActionTypes,
    DelayEnablePresentationModeDispatcher,
    DisablePresentationModeAction,
    EnablePresentationModeAction,
    SetAutoRefreshAction,
    SetAutoRefreshActionCreator,
    TemplateControlBarVisibilityToggledAction,
} from 'src/types/actions/app'

// ephemeral state action creators

export const enablePresentationMode = (): EnablePresentationModeAction => ({
  type: ActionTypes.EnablePresentationMode,
})

export const disablePresentationMode = (): DisablePresentationModeAction => ({
  type: ActionTypes.DisablePresentationMode,
})

export const delayEnablePresentationMode: DelayEnablePresentationModeDispatcher = () => async (
  dispatch: Dispatch<EnablePresentationModeAction>
): Promise<NodeJS.Timer> =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    notify(presentationMode())
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators

export const setAutoRefresh: SetAutoRefreshActionCreator = (
  milliseconds: number
): SetAutoRefreshAction => ({
  type: ActionTypes.SetAutoRefresh,
  payload: {
    milliseconds,
  },
})

export const templateControlBarVisibilityToggled = (): TemplateControlBarVisibilityToggledAction => ({
  type: ActionTypes.TemplateControlBarVisibilityToggled,
})
