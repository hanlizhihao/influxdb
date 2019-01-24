// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'
// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import StepSwitcher from 'src/dataLoaders/components/StepSwitcher'
// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
    clearSteps,
    decrementCurrentStepIndex,
    incrementCurrentStepIndex,
    setBucketInfo,
    setCurrentStepIndex,
    setSubstepIndex,
} from 'src/onboarding/actions/steps'

import {
    addConfigValue,
    addPluginBundleWithPlugins,
    clearDataLoaders,
    removeConfigValue,
    removePluginBundleWithPlugins,
    setActiveTelegrafPlugin,
    setConfigArrayValue,
    setDataLoadersType,
    setPluginConfiguration,
    updateTelegrafPluginConfig,
} from 'src/onboarding/actions/dataLoaders'
// Types
import {Links} from 'src/types/v2/links'
import {DataLoadersState, DataLoaderStep, DataLoaderType, Substep,} from 'src/types/v2/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import {Bucket} from 'src/api'
import PluginsSideBar from 'src/onboarding/components/PluginsSideBar'

export interface DataLoaderStepProps {
  links: Links
  currentStepIndex: number
  substep: Substep
  onSetCurrentStepIndex: (stepNumber: number) => void
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  onSetSubstepIndex: (index: number, subStep: number | 'streaming') => void
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
  visible: boolean
  bucket?: Bucket
  buckets: Bucket[]
  startingType?: DataLoaderType
  startingStep?: number
  startingSubstep?: Substep
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onSetConfigArrayValue: typeof setConfigArrayValue
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onSetSubstepIndex: typeof setSubstepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
}

interface StateProps {
  links: Links
  dataLoaders: DataLoadersState
  currentStepIndex: number
  substep: Substep
  username: string
  selectedBucket: string
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class DataLoadersWizard extends PureComponent<Props> {
  public componentDidMount() {
    this.handleSetBucketInfo()
    this.handleSetStartingValues()
  }

  public componentDidUpdate(prevProps: Props) {
    const hasBecomeVisible = !prevProps.visible && this.props.visible

    if (hasBecomeVisible) {
      this.handleSetBucketInfo()
      this.handleSetStartingValues()
    }
  }

  public render() {
    const {
      currentStepIndex,
      dataLoaders,
      dataLoaders: {telegrafPlugins},
      onSetDataLoadersType,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onAddPluginBundle,
      onRemovePluginBundle,
      onSetConfigArrayValue,
      visible,
      bucket,
      username,
      onSetBucketInfo,
      buckets,
      selectedBucket,
    } = this.props

    return (
      <WizardOverlay
        visible={visible}
        title={'Data Loading'}
        onDismis={this.handleDismiss}
      >
        <div className="wizard-contents">
          <PluginsSideBar
            telegrafPlugins={telegrafPlugins}
            onTabClick={this.handleClickSideBarTab}
            title="Plugins to Configure"
            visible={this.sideBarVisible}
            currentStepIndex={currentStepIndex}
          />
          <div className="wizard-step--container">
            <StepSwitcher
              currentStepIndex={currentStepIndex}
              onboardingStepProps={this.stepProps}
              bucketName={_.get(bucket, 'name', '')}
              selectedBucket={selectedBucket}
              dataLoaders={dataLoaders}
              onSetDataLoadersType={onSetDataLoadersType}
              onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
              onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
              onSetPluginConfiguration={onSetPluginConfiguration}
              onAddConfigValue={onAddConfigValue}
              onRemoveConfigValue={onRemoveConfigValue}
              onAddPluginBundle={onAddPluginBundle}
              onRemovePluginBundle={onRemovePluginBundle}
              onSetConfigArrayValue={onSetConfigArrayValue}
              onSetBucketInfo={onSetBucketInfo}
              org={_.get(bucket, 'organization', '')}
              username={username}
              buckets={buckets}
            />
          </div>
        </div>
      </WizardOverlay>
    )
  }

  private handleSetBucketInfo = () => {
    const {bucket, buckets} = this.props
    if (bucket || (buckets && buckets.length)) {
      const b = bucket || buckets[0]
      const {organization, organizationID, name, id} = b

      this.props.onSetBucketInfo(organization, organizationID, name, id)
    }
  }

  private handleSetStartingValues = () => {
    const {startingStep, startingType, startingSubstep} = this.props

    const hasStartingStep = startingStep || startingStep === 0
    const hasStartingSubstep = startingSubstep || startingSubstep === 0
    const hasStartingType =
      startingType || startingType === DataLoaderType.Empty

    if (hasStartingType) {
      this.props.onSetDataLoadersType(startingType)
    }

    if (hasStartingSubstep) {
      this.props.onSetSubstepIndex(
        hasStartingStep ? startingStep : 0,
        startingSubstep
      )
    } else if (hasStartingStep) {
      this.props.onSetCurrentStepIndex(startingStep)
    }
  }

  private handleDismiss = () => {
    this.props.onCompleteSetup()
    this.props.onClearDataLoaders()
    this.props.onClearSteps()
  }

  private get sideBarVisible() {
    const {dataLoaders, currentStepIndex} = this.props
    const {telegrafPlugins, type} = dataLoaders

    const isStreaming = type === DataLoaderType.Streaming
    const isNotEmpty = telegrafPlugins.length > 0
    const isConfigStep = currentStepIndex > 0

    return isStreaming && isNotEmpty && isConfigStep
  }

  private handleClickSideBarTab = (telegrafPluginID: string) => {
    const {
      onSetSubstepIndex,
      onSetActiveTelegrafPlugin,
      dataLoaders: {telegrafPlugins},
    } = this.props

    const index = Math.max(
      _.findIndex(telegrafPlugins, plugin => {
        return plugin.name === telegrafPluginID
      }),
      0
    )

    onSetSubstepIndex(DataLoaderStep.Configure, index)
    onSetActiveTelegrafPlugin(telegrafPluginID)
  }

  private get stepProps(): DataLoaderStepProps {
    const {
      links,
      notify,
      substep,
      onCompleteSetup,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      substep,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      links,
      notify,
      onCompleteSetup,
      onExit: this.handleDismiss,
    }
  }
}

const mstp = ({
  links,
  dataLoading: {
    dataLoaders,
    steps: {currentStep, substep, bucket},
  },
  me: {name},
}: AppState): StateProps => ({
  links,
  dataLoaders,
  currentStepIndex: currentStep,
  substep,
  username: name,
  selectedBucket: bucket,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
  onUpdateTelegrafPluginConfig: updateTelegrafPluginConfig,
  onAddConfigValue: addConfigValue,
  onRemoveConfigValue: removeConfigValue,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onAddPluginBundle: addPluginBundleWithPlugins,
  onRemovePluginBundle: removePluginBundleWithPlugins,
  onSetPluginConfiguration: setPluginConfiguration,
  onSetConfigArrayValue: setConfigArrayValue,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onSetSubstepIndex: setSubstepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DataLoadersWizard)
