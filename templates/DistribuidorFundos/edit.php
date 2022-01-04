<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DistribuidorFundo $distribuidorFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $distribuidorFundo->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $distribuidorFundo->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Distribuidor Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="distribuidorFundos form content">
            <?= $this->Form->create($distribuidorFundo) ?>
            <fieldset>
                <legend><?= __('Edit Distribuidor Fundo') ?></legend>
                <?php
                    echo $this->Form->control('cnpj');
                    echo $this->Form->control('nome');
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
