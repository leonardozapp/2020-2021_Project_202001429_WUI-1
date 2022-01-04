<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\SituacaoFundo $situacaoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Situacao Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="situacaoFundos form content">
            <?= $this->Form->create($situacaoFundo) ?>
            <fieldset>
                <legend><?= __('Add Situacao Fundo') ?></legend>
                <?php
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
